# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 13:04:19 2022
@author: pdelabarra
"""
from fastapi import FastAPI, Body, Depends
from fastapi.responses import JSONResponse
import requests
import json
import pandas as pd
from pydantic import BaseModel 
from typing import List, Union 
from fastapi.responses import PlainTextResponse
import logging
from datetime import datetime
import time
import smtplib
from dependencies import jwtBearer
import psycopg2
import pyodbc
from decouple import config

logging.basicConfig(filename="log.txt", 
                    level=logging.DEBUG
                    )

############################## CREDENCIALES DE ACCESO A SERVICIOS EXTERNOS################################
## Datos de seguridad para llamar a la base centralizada
basecentralizada__token_developer = config("basecentralizada__token_developer")
basecentralizada__base_url = config("basecentralizada__base_url")
base_url_icg = config("base_url_icg")
mbi__base_url = config("mbi__base_url")
icg_db_server = config("icg_db_server")
icg_db_database = config("icg_db_database")
icg_db_user = config("icg_db_user")
icg_db_password = config("icg_db_password")
basecentralizada_db_database = config("basecentralizada_db_database")
basecentralizada_db_user = config("basecentralizada_db_user")
basecentralizada_db_password = config("basecentralizada_db_password")
basecentralizada_db_server = config("basecentralizada_db_server")
basecentralizada_db_port = config("basecentralizada_db_port")
optimus_api_usuario = config("optimus_api_usuario")
optimus_api_password = config("optimus_api_password")
driver_sql = config("driver_sql")


############################## FORMATO DE LOS BODY DEL ORQUESTADOR ###################

## invocacion de la API
app = FastAPI(
                title = "Data Orchestrator MBI",
                description = "This API was built with FastAPI and exists to "+
                "populate all of the MBI legacy systems according to the data inputs "+
                "to the master database Base Centralizada MBI.\n To date only populates "+
                "Base Centralizada MBI which is the master database for clients attributes "+
                "and relationships, and ICG System, which the valuation system for portfolios.",
                version="0.2.1",
                # servers=[
                #             {
                #                 "url": "http://127.0.0.1:8000/",
                #                 "description": "Local Development Server"
                #             },
                #             {
                #                 "url": "https://mock.pstmn.io",
                #                 "description": "Mock Server",
                #             }
                #         ]
                )

############################## FORMATO DE LOS BODY DEL ORQUESTADOR ###################
## creando el formato de los body para la aplicacon y la creacion de valores en 
## todos los sistemas
class Value(BaseModel):
    name: str = 'APELLIDO_PATERNO'
    value: Union[str , int] = 'DE LA BARRA'
    index: Union[int, None] = 1
    
class ItemList(BaseModel):
    data: List[Value]
    
class Entity(BaseModel):
    name: Union[str, None] = 'PEDRO DE LA BARRA'     
    rut: Union[str, None] = '18928800-K'
    
class Portfolio(BaseModel):
    reference_id: Union[str, None] = 'PDELABARRA'     
    client_id: Union[int, None] = None
    company_id: Union[int, None] = None
    
class Relationship_Person(BaseModel):
    entity_from: Union[str, None] = 'client'
    entity_to: Union[str, None] = 'client'
    identifier_from: Union[int, None] = None
    identifier_to: Union[int,str, None] = None
    nombre_relacion: str = 'AGENTE'
    
class Relationship_Attribute(BaseModel):
    entity_from: Union[str, None] = 'client'
    entity_to: Union[str, None] = 'client'
    relationship_id:  Union[int, None] = 1
    name: Union[str, None] = 'ES_PRINCIPAL'
    value: Union[str, None] = 'SI'
    index: Union[int, None] = 1

############################## LOGIN ICG ################################
## llamado al token variable de cada invocacion de la API

## ejecución de la aplicacion y las definciones de cada uno de los servicios
## login de ICG para poder comenzar a consultar
def login(url = base_url_icg+"/api/login/authenticate", 
          usuario = "ICGAPI", password = "icgs1234"):
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({"Usuario": usuario, "Clave": password})
    req = requests.request("POST", url, headers=headers, data=payload)
    response = req.json()
    return response['Token']

##login de servicios de MBI
def login_mbi(url = mbi__base_url+"/v1/api/auth/login", 
              usuario = "UsrMbi_Test", password = "WW#$M!15"):
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({"Usuario": usuario, "Clave": password})
    req = requests.request("POST", url, headers=headers, data=payload)
    response = req.json()
    return response['Token']

##login de servicios de Optimus
def login_mbi(url = mbi__base_url+"/v1/api/auth/login", 
              usuario = "UsrMbi_Test", password = "WW#$M!15"):
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({"Usuario": usuario, "Clave": password})
    req = requests.request("POST", url, headers=headers, data=payload)
    response = req.json()
    return response['Token']

############################## ENVIAR MAIL  ################################
# funcion para enviar gmail desde el localhost
def enviar_gmail(texto, asunto = 'SERVICIOS ORQUESTADOR', destinatarios = ["pedro.delabarra@mbi.cl", 
                                                 "sofia.barrientos@mbi.cl"]):
    for i in destinatarios:
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login("pedro.delabarra@mbi.cl", "BB3XZpbd")
        message = 'Subject: {}\n\n{}'.format(asunto, texto.encode('utf-8'))
        s.sendmail("pedro.delabarra@mbi.cl", i, message)
        s.quit()
    return

############################## CONVERTIR DATE  ################################
# funcion para enviar gmail desde el localhost
def convert_date(date_string):
    try:
        # Try to convert the date string to a datetime object
        date_time = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
        # Format the date as "1994-11-30"
        formatted_date = date_time.strftime("%Y-%m-%d")
    except ValueError:
        # Try to convert the date string to a datetime object
        date_time = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        # Format the date as "1994-11-30"
        formatted_date = date_time.strftime("%Y-%m-%d")
    except TypeError:
        # Try to convert the date string to a datetime object
        date_time = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        # Format the date as "1994-11-30"
        formatted_date = date_time.strftime("%Y-%m-%d")
    return formatted_date

############################## SERVICIOS ORQUESTADOR ################################
## definicion de los servicios a usarse por el orquestador

@app.get("/basecentralizada__id_entity", response_class=PlainTextResponse,
         summary="Returns the ID of the entity from Base Centralizada using it Rut/Passport in for Clients and reference_id for portfolios", 
         description = "Retorna el RUT de la entidad que se consulta a nivel de ID",
         dependencies=[Depends(jwtBearer())],
         tags=["Base Centralizada"])
## obtener el ID de la entidad
def basecentralizada__id_entity(entity, variable):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(basecentralizada__id_entity.__name__, 
                                                                        (entity, variable))})
    url = basecentralizada__base_url+str(entity)+'/?limit=10000'
    headers = {'Authorization': basecentralizada__token_developer,
               'Content-Type': 'application/json'}
    req = requests.request("GET", url, headers=headers)
    response = req.json()
    try:
        if entity == 'portfolio':   
            for i in response['results']:
                if i['reference_id'] == variable:
                    aux = str(i['id'])
        else:   
             for i in response['results']:
                 if i['rut'] == variable:
                     aux = str(i['id'])
        return aux
    except:
        {"Mensaje": "No RUT en base centralizada"}

## obtener el rut de una entidad
@app.get("/basecentralizada__entity_rut", 
         summary="Returns the rut of the entity from Base Centralizada", 
         description = "Retorna el RUT de la entidad que se consulta a nivel de ID para Persona Natural o Juridica y el REFERENCE_ID"+
         "para portfolios",
         dependencies=[Depends(jwtBearer())],
         tags=["Base Centralizada"])
def basecentralizada__get__entity_rut(entity, id_entity):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(basecentralizada__get__entity_rut.__name__, 
                                                                        (entity, id_entity))})
    url = basecentralizada__base_url+str(entity)+'/id/'+str(id_entity)
    headers = {'Authorization': basecentralizada__token_developer,
               'Content-Type': 'application/json'}
    try:
        req = requests.request("GET", url, headers=headers)
        response = req.json()
        return (response['rut'], response['name'])
    except ValueError:  # This is the correct syntax
        return 'No existen datos asociados al cliente en Base Centralizada'
    except KeyError:  # This is the correct syntax
        return (response['reference_id'], response['client'], response['company'])
    # enviar_gmail("Se corrio el proceso de {0} con variables {1}".format(basecentralizada__get__entity_rut.__name__, 
    #                                                                     (entity, id_entity)))

## GET de los ID de los atributos relacionados a una relacion entre entidades
@app.get("/basecentralizada__get__relationship_attribute_id", 
         summary="Returns the ID of a particular relationship attribute from Base Centralizada", 
         description = "Retorna el ID de un atributo particular de una relacion. Por ejemplo el ID de FECHA_FIN en la relacion AGENTE entre Client-Client",
         dependencies=[Depends(jwtBearer())],
         tags=["Base Centralizada"])
def basecentralizada__get__relationship_attribute_id(entity_from, entity_to, name):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(basecentralizada__get__relationship_attribute_id.__name__, 
                                                                        (entity_from, entity_to))})
    if entity_from== 'client' and entity_to == 'client':
        url = basecentralizada__base_url+entity_to+'/relationship/attribute/'
    else:
        url = basecentralizada__base_url+entity_to+'/relationship/'+entity_from+'/attribute/'
    headers = {'Authorization': 'Api-Key 27nn29tw.WNwg7NTQFC82sDoiuRUqSCEt5WAZJa2M',
               'Content-Type': 'application/json'}
    req = requests.request("GET", url, headers=headers)
    try:
        response = req.json()
        for i in response['results']:
            if i['name'] == name:
                aux = i['id']
        # enviar_gmail("Se corrio el proceso de {0} con variables {1}".format(basecentralizada__get__relationship_attribute_id.__name__, 
        #                                                                     (entity_from, entity_to, name)), 
        #              basecentralizada__id_entity.__name__)
        return aux
    except:
        req.text
        

## FUNCIONAL: postear valores de atributos para las distintas entidades
@app.post("/orchestrator__create_value", response_class=PlainTextResponse,
          summary="Posts the values from the satellite system to Base Centralizada and all integrated system in the orchestrator",
          description = "Actualiza los atributos en todos los sistemas para entidades",
          dependencies=[Depends(jwtBearer())],
          tags=["Orchestrator MBI"])
def orchestrator__create_value(values: ItemList, entity, id_entity):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(orchestrator__create_value.__name__, 
                                                                        (values, entity, id_entity))})
    if entity == 'portfolio':
        output = dict()
        output['Base_Centralizada'] = {"Mensaje": "Servicio de creacion de Portfolio es otro"}
        return json.dumps(output)
    else:
        start_time = time.time()
        url = basecentralizada__base_url+str(entity)+'/id/'+str(id_entity)+'/detail/'
        headers = {'Authorization': basecentralizada__token_developer,
                   'Content-Type': 'application/json'}
        payload = values.json()
        response = requests.request("POST", url, headers=headers, data = payload)
        for i in values.data:
            if i.name == 'EMAIL':
                icg_atributos = icg__post__IngresarActualizarMail(entity, id_entity)
            if i.name == 'DIRECCION_CALLE':
                icg_atributos = icg__post__IngresarActualizarDireccion(entity, id_entity)
            if i.name == 'NUMERO_CUENTA_BANCARIA':
                icg_atributos = icg__post__IngresarActualizarCuentaBancaria(entity, id_entity)
            else:
                icg_atributos = json.dumps({"Mensaje": "No existe atributo en ICG"})
        icg_persona = icg__post__IngresarActualizarParticipes(entity, id_entity)
        output = dict()
        output['Base_Centralizada'] = json.loads(response.text)
        output['ICG_Entidad'] = json.loads(icg_persona)
        output['ICG_Atributos'] = json.loads(icg_atributos)
        output['Tiempo'] = str(time.time() - start_time)
        return json.dumps(output)

## FUNCIONAL: delete valores de atributos para las distintas entidades
@app.delete("/orchestrator__delete_value", response_class=PlainTextResponse,
            summary="Deletes the values from Base Centralizada and all integrated system in the orchestrator. ID_Attribute corresponds to the value ID, not the Attribute Type ID", 
            dependencies=[Depends(jwtBearer())],
            tags=["Orchestrator MBI"])
def orchestrator__delete_value(entity, id_entity, id_attribute):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(orchestrator__delete_value.__name__, 
                                                                        (entity, id_entity, id_attribute))})
    start_time = time.time()
    url = basecentralizada__base_url+str(entity)+'/id/'+str(id_entity)+'/detail/'+str(id_attribute)+'/'
    headers = {'Authorization': basecentralizada__token_developer,
               'Content-Type': 'application/json'}
    response = requests.request("DELETE", url, headers=headers)
    output = dict()
    output['Base_Centralizada'] = json.loads(response.text)
    output['ICG_Entidad'] = {"Mensaje": "No se hace uso"}
    output['ICG_Atributos'] = {"Mensaje": "No se hace uso"}
    output['Tiempo'] = str(time.time() - start_time)
    # enviar_gmail("Se corrio el proceso de {0} con variables {1}".format(orchestrator__delete_value.__name__, 
    #                                                                     (entity, id_entity, id_attribute)))
    return json.dumps(output)

## FUNCIONAL: crear una entidad de Client o Company dentro de los sistemas
@app.post("/orchestrator__create_entity", response_class=PlainTextResponse,
          summary="Creates entity Client or Company to all systems including Base Centralizada and all integrated systems integrated by the orchestrator",
          description = "Crea entidades del tipo Persona Natural o Persona Jurídica en Base Centralizada y en ICG",
          dependencies=[Depends(jwtBearer())],
          tags=["Orchestrator MBI"])
def orchestrator__create_entity(entidad: Entity, entity_type):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(orchestrator__delete_value.__name__, 
                                                                        (entidad, entity_type))})
    start_time = time.time()
    url = basecentralizada__base_url+entity_type+'/'
    headers = {'Authorization': basecentralizada__token_developer,
                'Content-Type': 'application/json'}
    payload = json.dumps({
                            "rut": entidad.rut, 
                            "name": entidad.name
                          })
    response = requests.request("POST", url, headers=headers, data=payload)
    id_entity = basecentralizada__id_entity(entity_type, entidad.rut)
    if entity_type == 'company':
        aux1 = 'RAZON_SOCIAL'
        aux2 = 'RUT_EMPRESA'
    else:
        aux1 = 'NOMBRE_COMPLETO'
        aux2 = 'RUT_PERSONA_NATURAL'
    payload2 = json.dumps({
                            "data": [
                                    {"name": aux1, "value": entidad.name, "index": 1},
                                    {"name": aux2, "value": entidad.rut, "index": 1}
                                    ]
                         })
    url2 = basecentralizada__base_url+entity_type+'/id/'+str(id_entity)+'/detail/'
    requests.request("POST", url2, headers=headers, data=payload2)
    icg_persona = icg__post__IngresarActualizarParticipes(entity_type, id_entity)
    output = dict()
    output['Base_Centralizada'] = json.loads(response.text)
    output['ICG_Entidad'] = json.loads(icg_persona)
    output['ICG_Atributos'] = {"Mensaje": "No se hace uso"}
    output['Tiempo'] = str(time.time() - start_time)
    # enviar_gmail("Se corrio el proceso de {0} con variables {1}".format(orchestrator__create_entity.__name__, 
    #                                                                     (entidad, entity_type)))
    return json.dumps(output)

## FUNCIONAL: crear una entidad de Client o Company dentro de los sistemas
@app.put("/orchestrator__update_entity", response_class=PlainTextResponse,
          summary="Updates an entity Client or Company to all systems including Base Centralizada and all integrated systems integrated by the orchestrator",
          description = "Actualiza entidades del tipo Persona Natural o Persona Jurídica en Base Centralizada y en ICG",
          dependencies=[Depends(jwtBearer())],
          tags=["Orchestrator MBI"])
def orchestrator__update_entity(entidad: Entity, entity, id_entity):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(orchestrator__update_entity.__name__, 
                                                                        (entidad, entity, id_entity))})
    start_time = time.time()
    url = basecentralizada__base_url+entity+'/id/'+str(id_entity)+'/'
    headers = {'Authorization': basecentralizada__token_developer,
                'Content-Type': 'application/json'}
    payload = json.dumps({
                            "rut": entidad.rut, 
                            "name": entidad.name
                          })
    response = requests.request("POST", url, headers=headers, data=payload)
    id_entity = basecentralizada__id_entity(entity, entidad.rut)
    if entity == 'company':
        aux1 = 'RAZON_SOCIAL'
        aux2 = 'RUT_EMPRESA'
    else:
        aux1 = 'NOMBRE_COMPLETO'
        aux2 = 'RUT_PERSONA_NATURAL'
    payload2 = json.dumps({
                            "data": [
                                    {"name": aux1, "value": entidad.name, "index": 1},
                                    {"name": aux2, "value": entidad.rut, "index": 1}
                                    ]
                         })
    url2 = basecentralizada__base_url+entity+'/id/'+str(id_entity)+'/detail/'
    requests.request("POST", url2, headers=headers, data=payload2)
    icg_persona = icg__post__IngresarActualizarParticipes(entity, id_entity)
    output = dict()
    output['Base_Centralizada'] = json.loads(response.text)
    output['ICG_Entidad'] = json.loads(icg_persona)
    output['ICG_Atributos'] = {"Mensaje": "No se hace uso"}
    output['Tiempo'] = str(time.time() - start_time)
    # enviar_gmail("Se corrio el proceso de {0} con variables {1}".format(orchestrator__update_entity.__name__, 
    #                                                                     (entidad, entity, id_entity)))
    return json.dumps(output)

## FUNCIONAL: crear un Portfolio dentro de todos los sistemas
@app.post("/orchestrator__create_portfolio", response_class=PlainTextResponse,
          summary="Creates entity PORTFOLIO to all systems including Base Centralizada and all integrated systems integrated by the orchestrator",
          description = "Crea entidades del tipo PORTFOLIO en Base Centralizada y en ICG",
          dependencies=[Depends(jwtBearer())],
          tags=["Orchestrator MBI"])
def orchestrator__create_portfolio(portfolio: Portfolio, Tipo_Consolidado = 'Individual'):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(orchestrator__create_portfolio.__name__, 
                                                                        (portfolio, Tipo_Consolidado))})
    start_time = time.time()
    url = basecentralizada__base_url+'portfolio/'
    headers = {'Authorization': basecentralizada__token_developer,
                'Content-Type': 'application/json'}
    if portfolio.client_id != None:
        payload = json.dumps({
                            "reference_id": portfolio.reference_id,
                            "client_id": portfolio.client_id
                              })
    else:
        payload = json.dumps({
                            "reference_id": portfolio.reference_id,
                            "company_id": portfolio.company_id
                              })
    response = requests.request("POST", url, headers=headers, data=payload)
    id_portfolio = basecentralizada__id_entity('portfolio', portfolio.reference_id)
    payload2 = json.dumps({
                            "data": [
                                    { "name": "TIPO_CONSOLIDACION_PORTFOLIO", "value": Tipo_Consolidado, "index": 1}
                                    ]
                         })
    url2 = basecentralizada__base_url+'portfolio/id/'+str(id_portfolio)+'/detail/'
    requests.request("POST", url2, headers=headers, data=payload2)
    if portfolio.client_id != None:
        if Tipo_Consolidado == 'Individual':
            icg_atributos = icg__post__CrearActualizarCuentaIndividual(portfolio.reference_id, id_portfolio,  client_id = portfolio.client_id)
        else:
            icg_atributos = icg__post__CrearCuentaConsolidada(portfolio.reference_id, id_portfolio, client_id = portfolio.client_id)
    else:
        if Tipo_Consolidado == 'Individual':
            icg_atributos = icg__post__CrearActualizarCuentaIndividual(portfolio.reference_id, id_portfolio, company_id = portfolio.company_id)
        else:
            icg_atributos = icg__post__CrearCuentaConsolidada(portfolio.reference_id, id_portfolio, company_id = portfolio.company_id)
    output = dict()
    output['Base_Centralizada'] = json.loads(response.text)
    output['ICG_Entidad'] = {"Mensaje": "No se hace uso"}
    output['ICG_Atributos'] = json.loads(icg_atributos)
    output['Tiempo'] = str(time.time() - start_time)
    # enviar_gmail("Se corrio el proceso de {0} con variables {1}".format(orchestrator__create_portfolio.__name__, 
    #                                                                     (portfolio, Tipo_Consolidado)))
    return json.dumps(output)

## retorna los valores de los atributos de las entidades creadas
@app.get("/basecentralizada__get__attribute_detail",
          summary="Get the attributes value from base de datos centralizada for each of the attributes",
          description = "Crea relaciones en Base Centralizada y en ICG",
          dependencies=[Depends(jwtBearer())],
          tags=["Base Centralizada"])
def basecentralizada__get__attributes(entity, id_entity):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(basecentralizada__get__attributes.__name__, 
                                                                        (entity, id_entity))})
    url = basecentralizada__base_url+str(entity)+'/id/'+str(id_entity)+'/detail?limit=100000'
    headers = {'Authorization': basecentralizada__token_developer,
               'Content-Type': 'application/json'}
    try:
        req = requests.request("GET", url, headers=headers)
        aux = dict()
        response = req.json()
        for i in response['results']:
            if entity == 'client':
                aux[i['attribute']['name']] = i['data']
            if entity == 'company':
                aux[i['company_attribute']['name']] = i['data']
            if entity == 'portfolio':
                aux[i['attribute']['name']] = i['data']
        return aux
    except ValueError:  # This is the correct syntax
        return 'No existen datos asociados al cliente en Base Centralizada'

## FUNCIONAL: postear relacion entre dos entidades
@app.post("/basecentralizada__post__relationship", response_class=PlainTextResponse,
          summary="Creates relationship between entities all systems including Base Centralizada and all integrated systems integrated by the orchestrator",
          description = "Crea relaciones en Base Centralizada y en ICG",
          dependencies=[Depends(jwtBearer())],
          tags=["Orchestrator MBI"])
def basecentralizada__post__relationship(relationship: Relationship_Person):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(basecentralizada__post__relationship.__name__, 
                                                                        (relationship))})
    start_time = time.time()
    headers = {'Authorization': basecentralizada__token_developer,
               'Content-Type': 'application/json'}
    if relationship.entity_to == 'company':
        url = basecentralizada__base_url+str(relationship.entity_to)+"/id/"+str(relationship.identifier_to)+"/relationship/"
        payload = json.dumps({
                              "client_from": relationship.identifier_from,
                              "company_to": relationship.identifier_to,
                              "relationship_name": relationship.nombre_relacion
                            })
    if relationship.entity_to == 'client':
        url = basecentralizada__base_url+str(relationship.entity_from)+"/id/"+str(relationship.identifier_from)+"/relationship/"
        payload = json.dumps({
                              "client_from": relationship.identifier_from,
                              "client_to": relationship.identifier_to,
                              "relationship_name": relationship.nombre_relacion
                            })
    if relationship.entity_to == 'portfolio':
        url = basecentralizada__base_url+str(relationship.entity_from)+"/id/"+str(relationship.identifier_from)+"/portfolio/relationship/"
        if relationship.entity_from == 'company':
            payload = json.dumps({
                                  "company_from": relationship.identifier_from,
                                  "portfolio_to": relationship.identifier_to,
                                  "relationship_name": relationship.nombre_relacion
                                })
        else:
            payload = json.dumps({
                                  "client_from": relationship.identifier_from,
                                  "portfolio_to": relationship.identifier_to,
                                  "relationship_name": relationship.nombre_relacion
                                })
    try:
        icg_auxiliar = json.loads(icg__post__asociarPersonas(relationship))
    except:
        icg_auxiliar = {"Mensaje": "No se hace uso"}
    output = dict()
    try:
        response = requests.request("POST", url, headers=headers, data=payload)

        output['Base_Centralizada'] = json.loads(response.text)
        output['ICG_Entidad'] = {"Mensaje": "No se hace uso"}    
        output['ICG_Atributos'] = icg_auxiliar
        output['Tiempo'] = str(time.time() - start_time)
        # enviar_gmail("Se corrio el proceso de {0} con variables {1}".format(basecentralizada__post__relationship.__name__, 
        #                                                                     (relationship)))
        return json.dumps(output)
    except:
        output['ICG_Entidad'] = {"Mensaje": "Error de parametros"}
        return json.dumps(output)

## FUNCIONAL: postear relacion entre dos entidades
@app.post("/basecentralizada__post__relationship_attribute", response_class=PlainTextResponse,
          summary="Creates relationship attribute between entities all systems including Base Centralizada and all integrated systems integrated by the orchestrator",
          description = "Crea atributos de relaciones en Base Centralizada y en ICG",
          dependencies=[Depends(jwtBearer())],
          tags=["Orchestrator MBI"])
def basecentralizada__post__attribute_rel_value(relationship: Relationship_Attribute):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(basecentralizada__post__attribute_rel_value.__name__, 
                                                                        (relationship))})
    aux = basecentralizada__get__relationship_attribute_id(relationship.entity_from, relationship.entity_to, relationship.name)
    if relationship.entity_to == 'portfolio':
        url = 'https://mbi-bc.symlab.io/'+relationship.entity_to+'/relationship/'+relationship.entity_from+'/id/'+str(relationship.relationship_id)+'/detail/'+str(aux)+'/'
    else:
        url = 'https://mbi-bc.symlab.io/'+relationship.entity_to+'/relationship/id/'+str(relationship.relationship_id)+'/detail/'+str(aux)+'/'
    headers = {'Authorization': 'Api-Key 27nn29tw.WNwg7NTQFC82sDoiuRUqSCEt5WAZJa2M',
               'Content-Type': 'application/json'}
    payload = json.dumps({
                          "data": [
                                    {
                                      "name": relationship.name,
                                      "value": relationship.value,
                                      "index": relationship.index
                                    }
                                  ]
                                })
    response = requests.request("POST", url, headers=headers, data=payload)
    output = dict()
    output['Base_Centralizada'] = json.loads(response.text)
    output['ICG_Entidad'] = {"Mensaje": "No se hace uso"}
    output['ICG_Atributos'] = {"Mensaje": "No se hace uso"}
    # enviar_gmail("Se corrio el proceso de {0} con variables {1}".format(basecentralizada__post__attribute_rel_value.__name__, 
    #                                                                     (relationship)))
    return json.dumps(output)


## retornar el nombre del atributo dado un valor
def basecentralizada__get__attribute_name(entity, attribute_id):
    url = basecentralizada__base_url+str(entity)+'/attribute/'+str(attribute_id)
    headers = {'Authorization': basecentralizada__token_developer,
               'Content-Type': 'application/json'}
    req = requests.request("GET", url, headers=headers)
    response = req.json()
    return response['name']

## FUNCIONAL: get de la lista completa de los ENUM
@app.get("/basecentralizada__get__enum_attribute", response_class=PlainTextResponse,
          summary="List all the possible values for a ENUM attribute ",
          description = "Lista los atributos posibles para un atributo de una entidad en particular",
          dependencies=[Depends(jwtBearer())],
          tags=["Base Centralizada"])
## retornar los enumerables posibles para un atributo ENUM acode a un atributo de una entidad
def basecentralizada__get__enum_attribute(entity, attribute_id):
    logging.info({"Se corrio el proceso de {0} con variables {1}".format(basecentralizada__get__enum_attribute.__name__, 
                                                                        (entity, attribute_id))})
    url = basecentralizada__base_url+str(entity)+'/enum/attribute/'+str(attribute_id)
    headers = {'Authorization': basecentralizada__token_developer,
               'Content-Type': 'application/json'}
    response = requests.request("GET", url, headers=headers)
    output = dict()
    output['Base_Centralizada'] = json.loads(response.text)
    output['ICG_Entidad'] = {"Mensaje": "No se hace uso"}
    output['ICG_Atributos'] = {"Mensaje": "No se hace uso"}
    return json.dumps(output)

############################## SERVICIOS INTERMEDIOS ################################
## servicios intermedios para poder convertir datos desde una lógica a otra

## Diccionario para transformar atributos desde BaseCentralizada a ICG
def null_attributes(sistema_origen, valor):
    diccionario_isnull = pd.read_excel("Diccionario_Integraciones.xlsx"
                                       ,sheet_name = "isNULL")
    diccionario_integraciones = diccionario_isnull.apply(lambda x: x.astype(str).str.upper())
    diccionario = diccionario_integraciones[(diccionario_integraciones['Sistema'] == sistema_origen)]
    try:
        aux = diccionario['Valor_Destino'][diccionario['Columna_Origen'] == valor].iloc[0]
    except IndexError:
        aux = valor
    return aux

## Diccionario para transformar atributos desde BaseCentralizada a ICG
def convertir_atributos_basecentralizada_icg(sistema_origen, sistema_destino, lista):
    diccionario_integraciones = pd.read_excel("Diccionario_Integraciones.xlsx"
                                              ,sheet_name = "Diccionario_Sistemas")
    diccionario_integraciones = diccionario_integraciones.apply(lambda x: x.astype(str).str.upper())
    diccionario = diccionario_integraciones[(diccionario_integraciones['Sistema_Origen'] == sistema_origen) &
                                            (diccionario_integraciones['Sistema_Destino'] == sistema_destino)]
    dictionary_transformed = dict()
    try:
        for i in lista.keys():
            try:
                aux = diccionario['Valor_Destino'][diccionario['Valor_Origen'] == lista[i]].iloc[0]
            except IndexError:
                aux = lista[i]
            dictionary_transformed[i] = aux
    except AttributeError:
        dictionary_transformed[lista] = lista
    return dictionary_transformed

############################## SERVICIOS DE ICG ################################
## funciones a partir de los servicios de ICG para la creacion en orquestador

## GET de si existe un participe en ICG o no, en caso contrario lo debe crear
def icg__get__Participes(rut):
    headers = {'Authorization': 'Bearer '+login(), 'Content-Type': 'application/json'}
    url = base_url_icg+"/api/persona/LeerParticipe"
    payload = json.dumps({
                        "Rut_Participe": rut
                        })
    req = requests.request("GET", url, headers=headers, data=payload)
    response = req.json()
    if 'Rut_Participe' in response.keys():
        aux = True
    else:
         aux = False   
    return aux

## POST Email a ICG desde base centralizada
def icg__post__IngresarActualizarMail(entity, id_entity):
    lista = basecentralizada__get__attributes(entity, id_entity)
    icg_data = convertir_atributos_basecentralizada_icg('BASE CENTRALIZADA', 'ICG', lista)
    rut = basecentralizada__get__entity_rut(entity, id_entity)[0]
    if icg__get__Participes(rut) == True:
        url = base_url_icg+"/api/persona/ActualizarEmail"
        method = 'PUT'
    else:
        url = base_url_icg+"/api/persona/IngresarEmail"
        method = 'POST'
    headers = {'Authorization': 'Bearer '+login(), 
               'Content-Type': 'application/json'}
    payload = json.dumps({
                          "Rut": rut,
                          "Tipo_Email": null_attributes('ICG',icg_data.get('TIPO_EMAIL', 'TIPO_EMAIL')),
                          "Email": null_attributes('ICG',icg_data.get('EMAIL', 'EMAIL')),
                          "Es_Principal":null_attributes('ICG',icg_data.get('EMAIL_PRINCIPAL', 'EMAIL_PRINCIPAL'))
                          })
    response = requests.request(method, url, headers=headers, data=payload)
    return response.text

## POST Direccion a ICG desde base centralizada
def icg__post__IngresarActualizarDireccion(entity, id_entity):
    lista = basecentralizada__get__attributes(entity, id_entity)
    icg_data = convertir_atributos_basecentralizada_icg('BASE CENTRALIZADA', 'ICG', lista)
    rut = basecentralizada__get__entity_rut(entity, id_entity)[0]
    if icg__get__Participes(rut) == True:
        url = base_url_icg+"/api/persona/ActualizarDireccion"
        method = 'PUT'
    else:
        url = base_url_icg+"/api/persona/IngresarDireccion"
        method = 'POST'
    headers = {'Authorization': 'Bearer '+login(), 
               'Content-Type': 'application/json'}
    payload = json.dumps({
                          "Rut": rut,
                          "Tipo_Direccion": null_attributes('ICG', icg_data.get('DIRECCION_TIPO', 'DIRECCION_TIPO')),
                          "Direccion": null_attributes('ICG',icg_data.get('DIRECCION_CALLE', 'DIRECCION_CALLE')),
                          "Comuna": null_attributes('ICG',icg_data.get('DIRECCION_CALLE', 'DIRECCION_CALLE')),
                          "Ciudad": null_attributes('ICG',icg_data.get('DIRECCION_CIUDAD', 'DIRECCION_CIUDAD')),
                          "Telefono_Fijo": null_attributes('ICG',icg_data.get('TELEFONO', 'TELEFONO')),
                          "Telefono_Movil": null_attributes('ICG',icg_data.get('TELEFONO', 'TELEFONO')),
                          "Es_Principal": null_attributes('ICG',icg_data.get('TELEFONO_PRINCIPAL', 'TELEFONO_PRINCIPAL'))
                          })
    response = requests.request(method, url, headers=headers, data=payload)
    return response.text

## POST Direccion a ICG desde base centralizada
def icg__post__IngresarActualizarCuentaBancaria(entity, id_entity):
    lista = basecentralizada__get__attributes(entity, id_entity)
    icg_data = convertir_atributos_basecentralizada_icg('BASE CENTRALIZADA', 'ICG', lista)
    rut = basecentralizada__get__entity_rut(entity, id_entity)[0]
    if icg__get__Participes(rut) == True:
        url = base_url_icg+"/api/persona/ActualizarCuentaBancaria"
        method = 'PUT'
    else:
        url = base_url_icg+"/api/persona/IngresarCuentaBancaria"
        method = 'POST'
    headers = {'Authorization': 'Bearer '+login(), 
               'Content-Type': 'application/json'}
    payload = json.dumps({
                          "Rut_Participe": rut,
                          "Codigo_Bco": null_attributes('ICG',icg_data.get('BANCO_CUENTA_BANCARIA', 'BANCO_CUENTA_BANCARIA')),
                          "Nro_Cuenta": null_attributes('ICG',icg_data.get('NUMERO_CUENTA_BANCARIA', 'NUMERO_CUENTA_BANCARIA')),
                          "Tipo_Cuenta": null_attributes('ICG',icg_data.get('TIPO_CUENTA_BANCARIA', 'TIPO_CUENTA_BANCARIA')),
                          "Moneda": null_attributes('ICG',icg_data.get('MONEDA_CUENTA_BANCARIA', 'MONEDA_CUENTA_BANCARIA')),
                          "Sucursal": 'NA',
                          "Ejecutivo": 'NA',
                          "Telefono": '12345',
                          "Correo_Electronico": null_attributes('ICG',icg_data.get('EMAIL', 'EMAIL')),
                          "Es_Principal": null_attributes('ICG',icg_data.get('CUENTA_BANCARIA_PRINCIPAL', 'CUENTA_BANCARIA_PRINCIPAL')),
                        })
    response = requests.request(method, url, headers=headers, data=payload)
    return response.text

## DELETE del mail desde ICG a Base Centralizada
def icg__delete__EliminaMail(entity, id_entity):
    rut = basecentralizada__get__entity_rut(entity, id_entity)[0]
    url = base_url_icg+"/api/persona/EliminarEmail"
    headers = {'Authorization': 'Bearer '+login(), 
               'Content-Type': 'application/json'}
    aux = convertir_atributos_basecentralizada_icg('BASE CENTRALIZADA', 'ICG', 
                                                   basecentralizada__get__attributes(entity, id_entity))
    if len(aux) != 0:
        payload = json.dumps({
                                "Rut_Participe": rut,
                                "Tipo_Email": convertir_atributos_basecentralizada_icg('BASE CENTRALIZADA', 'ICG', 
                                                                                       basecentralizada__get__attributes(entity, id_entity))
                            })
        response = requests.request('DELETE', url, headers=headers, data=payload)
        return response.text
    else:
        return

## POST para crear entidades de Participes en ICG
def icg__post__IngresarActualizarParticipes(entity, id_entity):
    headers = {'Authorization': 'Bearer '+login(), 'Content-Type': 'application/json'}
    lista = basecentralizada__get__attributes(entity, id_entity)
    rut = basecentralizada__get__entity_rut(entity, id_entity)[0]
    icg_data = convertir_atributos_basecentralizada_icg('BASE CENTRALIZADA', 'ICG', lista)
    if entity == 'client':
        if icg__get__Participes(rut) == True:
            url = base_url_icg+"/api/persona/ActualizarParticipeNatural"
            method = 'PUT'
        else:
            url = base_url_icg+"/api/persona/IngresarParticipeNatural"
            method = 'POST'
        payload = json.dumps({
                              "Rut_Participe": rut,
                              "Nacionalidad": null_attributes('ICG',icg_data.get('NACIONALIDAD', 'NACIONALIDAD')),
                              "Fecha_Nacimiento": convert_date(null_attributes('ICG',icg_data.get('FECHA_NACIMIENTO', 'FECHA_NACIMIENTO'))),
                              "Sexo": null_attributes('ICG',icg_data.get('SEXO', 'SEXO')),
                              "Estado_Civil": "S",
                              "Rut_Empleador": "",
                              "Nombre_Empleador": "",
                              "Cargo": null_attributes('ICG',icg_data.get('CARGO', 'CARGO')),
                              "Lugar_EnvioCorresp": "P",
                              "Rut_Conyuge": "",
                              "Nombre_1": null_attributes('ICG',icg_data.get('NOMBRE_1', 'NOMBRE_1')),
                              "Nombre_2": null_attributes('ICG',icg_data.get('NOMBRE_2', 'NOMBRE_2')),
                              "Apellido_Paterno": null_attributes('ICG',icg_data.get('APELLIDO_PATERNO', 'APELLIDO_PATERNO')),
                              "Apellido_Materno": null_attributes('ICG',icg_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                              "Pais_Residencia": null_attributes('ICG',icg_data.get('PAIS_RESIDENCIA', 'PAIS_RESIDENCIA')),
                              "Pasaporte": "NA",
                              "Actividad": null_attributes('ICG',icg_data.get('ACTIVIDAD', 'ACTIVIDAD')),
                              "Nivel_Educacional": null_attributes('ICG',icg_data.get('NIVEL_EDUCACIONAL', 'NIVEL_EDUCACIONAL')),
                              "Condicion_Laboral": null_attributes('ICG',icg_data.get('CONDICION_LABORAL', 'CONDICION_LABORAL')),
                              "Regimen_Conyugal": "PART_GAN",
                              "Perfil_Inv": "CO",
                              "Cargo_PEP": "CARGO_5",
                              "PEP_Relacionado": "OTR",
                              "Es_Vinc_PEP": "N",
                              "Origen_Fondos": "VEN_ACT",
                              "Relacion_ConAdm": "CLI_EMP_REL"
                            })
    else:
        if icg__get__Participes(rut) == True:
            url = base_url_icg+"/api/persona/ActualizarParticipeJuridica"
            method = 'PUT'
        else:
            url = base_url_icg+"/api/persona/IngresarParticipeJuridica"
            method = 'POST'
        payload = json.dumps({
                            "Rut_Participe": rut,
                            "Razon_Social" : null_attributes('ICG',icg_data.get('RAZON_SOCIAL', 'RAZON_SOCIAL')),
                            "Pais_Constitucion": null_attributes('ICG',icg_data.get('PAIS_CONSTITUCION', 'PAIS_CONSTITUCION')),
                            "Fecha_Constitucion": convert_date(null_attributes('ICG',icg_data.get('FECHA_CONSTITUCION', 'FECHA_CONSTITUCION'))),
                            "Lugar_EnvioCorresp":"P",
                            "Giro" : null_attributes('ICG',icg_data.get('GIRO', 'GIRO')),
                            "Tipo_Sociedad" :"COL",
                            "Perfil_Inv" : null_attributes('ICG',icg_data.get('PERFIL', 'PERFIL')),
                            "Origen_Fondos" :"VEN_ACT",
                            "Relacion_ConAdm" :"CLI_EMP_REL"
                            })
    response = requests.request(method, url, headers=headers, data=payload)
    return response.text

## POST de crear portafolios en ICG
def icg__post__CrearActualizarCuentaIndividual(reference_id, portfolio_id, client_id = None, company_id = None):
    headers = {'Authorization': 'Bearer '+login(), 'Content-Type': 'application/json'}
    url = base_url_icg+"/api/admCartera/CrearCuentaIndividual"
    lista = basecentralizada__get__attributes('portfolio', portfolio_id)
    icg_data = convertir_atributos_basecentralizada_icg('BASE CENTRALIZADA', 'ICG', lista)
    if client_id != None:
        aux1 = 'client'
        aux2 =  client_id
    else:
        aux1 = 'company'
        aux2 = company_id
    rut = basecentralizada__get__entity_rut(aux1, aux2)[0]
    name = basecentralizada__get__entity_rut(aux1, aux2)[1]
    payload = json.dumps({
                        "Codigo_Fdo": reference_id,
                        "Codigo_AGF" : null_attributes('ICG',icg_data.get('CODIGO_AGF', 'CODIGO_AGF')),
                        "Moneda_Fdo" : null_attributes('ICG',icg_data.get('MONEDA_PORTFOLIO', 'MONEDA_PORTFOLIO')),
                        "Rut_Fondo" : rut,
                        "Nombre_Fdo" : name,
                        "Descripcion" : 'Creado via API',
                        "Fecha_Apertura" : datetime.today().strftime('%Y-%m-%d'),
                        "Tiene_Contabilidad" : "S",
                        "Perfil" : null_attributes('ICG',icg_data.get('PERFIL_PORTFOLIO', 'PERFIL_PORTFOLIO')),
                        "Tipo_Precio" : "M"
                        })
    req = requests.request("POST", url, headers=headers, data=payload)
    response = req.json()
    if response["Estado"] == True:
        aux = req.text
    else:
        req2 = requests.request("POST", base_url_icg+"/api/admCartera/ActualizaCuentaIndividual", 
                                headers=headers, data=payload)
        aux = req2.text
    return aux
    
## POST para crear cuentas consolidadas en ICG entre multiples cuentas
def icg__post__CrearCuentaConsolidada(reference_id, portfolio_id, client_id = None, company_id = None):
    headers = {'Authorization': 'Bearer '+login(), 'Content-Type': 'application/json'}
    url = base_url_icg+"/api/admCartera/CrearCuentaConsolidada"
    lista = basecentralizada__get__attributes('portfolio', basecentralizada__id_entity('portfolio', reference_id))
    icg_data = convertir_atributos_basecentralizada_icg('BASE CENTRALIZADA', 'ICG', lista)
    if client_id != None:
        aux1 = 'client'
        aux2 =  client_id
    else:
        aux1 = 'company'
        aux2 = company_id
    rut = basecentralizada__get__entity_rut(aux1, aux2)[0]
    payload = json.dumps({
                        "Codigo_Fdo" : null_attributes('ICG',icg_data.get('Codigo_AGF', 'Codigo_AGF')),
                        "Codigo_AGF" : null_attributes('ICG',icg_data.get('Codigo_AGF', 'Codigo_AGF')),
                        "Moneda_Fdo" : null_attributes('ICG',icg_data.get('MONEDA_PORTFOLIO', 'MONEDA_PORTFOLIO')),
                        "Rut_Fondo" : rut,
                        "Nombre_Fdo" : null_attributes('ICG',icg_data.get('NOMBRE_PORTFOLIO', 'NOMBRE_PORTFOLIO')),
                        "Descripcion" : 'Creado via API - ',
                        "Fecha_Apertura" : datetime.today().strftime('%Y-%m-%d')
                        })
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

## POST relacion representate legal y Agente ICG
def icg__post__asociarPersonas(relationship: Relationship_Person):
    icg__post__crearPersonas(relationship)
    headers = {'Authorization': 'Bearer '+login(), 'Content-Type': 'application/json'}
    if relationship.nombre_relacion == 'AGENTE': 
        url = base_url_icg+"/api/persona/AsociarAgente"
        payload = json.dumps({
                              "Rut_Participe": basecentralizada__get__entity_rut(relationship.entity_to, relationship.identifier_to)[0],
                              "Rut_Agente": basecentralizada__get__entity_rut(relationship.entity_from, relationship.identifier_from)[0],
                              "Fecha_Ini": datetime.today().strftime('%Y-%m-%d'),
                              "Fecha_Fin": "2100-01-01"
                            })
    if relationship.nombre_relacion == 'REPRESENTANTE_LEGAL': 
            url = base_url_icg+"/api/persona/AsociarRepresentanteLegal"
            payload = json.dumps({
                                  "Rut_Participe": basecentralizada__get__entity_rut(relationship.entity_to, relationship.identifier_to)[0],
                                  "Rut_Representante": basecentralizada__get__entity_rut(relationship.entity_from, relationship.identifier_from)[0],
                                  "Tipo_Actuacion": "I",
                                  "Fecha_Ini": datetime.today().strftime('%Y-%m-%d'),
                                  "Fecha_Fin": "2100-01-01"
                                })
    if relationship.nombre_relacion == 'AUTORIZADOR_ORDENES':
        url = base_url_icg+"/api/persona/AsociarAutorizadorOrdenes"
        payload = json.dumps({
                              "Rut_Participe": basecentralizada__get__entity_rut(relationship.entity_to, relationship.identifier_to)[0],
                              "Rut_Autorizador": basecentralizada__get__entity_rut(relationship.entity_from, relationship.identifier_from)[0],
                              "Tipo_Orden": "TD",
                              "Fecha_Ini": datetime.today().strftime('%Y-%m-%d'),
                              "Fecha_Fin": "2100-01-01"
                            })
    if relationship.nombre_relacion in ('AUTORIZADOR_ORDENES', 'REPRESENTANTE_LEGAL', 'AGENTE'):
        response = requests.request("POST", url, headers=headers, data=payload)
        return response.text
    else:
        pass
        return 'ICG no contiene la relacion, no ejecuto nada'
    
## POST de crear personas del tipo RepLegal, TomadorDecision, Agente
def icg__post__crearPersonas(relationship: Relationship_Person):
    headers = {'Authorization': 'Bearer '+login(), 'Content-Type': 'application/json'}
    aux = basecentralizada__get__entity_rut(relationship.entity_from, relationship.identifier_from)
    rut = aux[0]
    nombre = aux[1]
    lista = basecentralizada__get__attributes(relationship.entity_from, relationship.identifier_from)
    icg_data = convertir_atributos_basecentralizada_icg('BASE CENTRALIZADA', 'ICG', lista)
    if relationship.nombre_relacion == 'AGENTE':
        if relationship.entity_from == 'company':
            # if icg__get__Participes(rut) == True:
            #     url = base_url_icg+"/api/persona/ActualizarAgentePersonaJuridica"
            #     method = 'PUT'
            # else:
            url = base_url_icg+"/api/persona/IngresarAgentePersonaJuridica"
            method = 'POST'
            payload = json.dumps({
                                  "Rut_Agente":  rut, 
                                  "Razon_Social": null_attributes('ICG',icg_data.get('RAZON_SOCIAL', 'RAZON_SOCIAL')),
                                  "Pais_Constitucion":  null_attributes('ICG',icg_data.get('PAIS_CONSTITUCION', 'PAIS_CONSTITUCION')),
                                  "Fecha_Constitucion": convert_date(null_attributes('ICG',icg_data.get('FECHA_CONSTITUCION', 'FECHA_CONSTITUCION'))),
                                  "Tipo_Sociedad": null_attributes('ICG',icg_data.get('TIPO_SOCIEDAD', 'TIPO_SOCIEDAD')),
                                  "Codigo_Agente": null_attributes('ICG',icg_data.get('RAZON_SOCIAL', 'RAZON_SOCIAL'))[0:2],
                                  "Codigo_Agencia":  'MBIINV',
                                  "Codigo_Canal":  'MBIINV'
                                })
        else:
            # if icg__get__Participes(rut) == True:
            #     url = base_url_icg+"/api/persona/ActualizarAgentePersonaNatural"
            #     method = 'PUT'
            # else:
            url = base_url_icg+"/api/persona/IngresarAgentePersonaNatural"
            method = 'POST'
            payload = json.dumps({
                                  "Rut_Agente": rut,
                                  "Nacionalidad": null_attributes('ICG',icg_data.get('NACIONALIDAD', 'NACIONALIDAD')),
                                  "Fecha_Nacimiento": convert_date(null_attributes('ICG',icg_data.get('FECHA_NACIMIENTO', 'FECHA_NACIMIENTO'))),
                                  "Sexo": null_attributes('ICG',icg_data.get('SEXO', 'SEXO')),
                                  "Nombre_1":null_attributes('ICG',icg_data.get('NOMBRE_1', 'NOMBRE_1')),
                                  "Nombre_2": null_attributes('ICG',icg_data.get('NOMBRE_2', 'NOMBRE_2')),
                                  "Apellido_Paterno": null_attributes('ICG',icg_data.get('APELLIDO_PATERNO', 'APELLIDO_PATERNO')),
                                  "Apellido_Materno": null_attributes('ICG',icg_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                                  "Codigo_Agente": nombre[0:2],
                                  "Codigo_Agencia": 'MBIINV',
                                  "Codigo_Canal": 'MBIINV'
                                })        
    if relationship.nombre_relacion == 'REPRESENTANTE_LEGAL':
        # if icg__get__Participes(rut) == True:
        #     url = base_url_icg+"/api/persona/ActualizarRepresentanteLegal"
        #     method = 'PUT'
        # else:
        url = base_url_icg+"/api/persona/IngresarRepresentanteLegal"
        method = 'POST'
        payload = json.dumps({
                               "Rut_Participe": rut,
                               "Nacionalidad": null_attributes('ICG',icg_data.get('NACIONALIDAD', 'NACIONALIDAD')),
                               "Fecha_Nacimiento": convert_date(null_attributes('ICG',icg_data.get('FECHA_NACIMIENTO', 'FECHA_NACIMIENTO'))),
                               "Sexo": null_attributes('ICG',icg_data.get('SEXO', 'SEXO')),
                               "Estado_Civil": null_attributes('ICG',icg_data.get('ESTADO_CIVIL', 'ESTADO_CIVIL')),
                               "Rut_Empleador": "",
                               "Nombre_Empleador": "",
                               "Cargo": null_attributes('ICG',icg_data.get('CARGO', 'CARGO')),
                               "Lugar_EnvioCorresp": "P",
                               "Rut_Conyuge": "",
                               "Nombre_1": null_attributes('ICG',icg_data.get('NOMBRE_1', 'NOMBRE_1')),
                               "Nombre_2": null_attributes('ICG',icg_data.get('NOMBRE_2', 'NOMBRE_2')),
                               "Apellido_Paterno": null_attributes('ICG',icg_data.get('APELLIDO_PATERNO', 'APELLIDO_PATERNO')),
                               "Apellido_Materno": null_attributes('ICG',icg_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                               "Pais_Residencia": null_attributes('ICG',icg_data.get('PAIS_RESIDENCIA', 'PAIS_RESIDENCIA')),
                               "Pasaporte": "NA",
                               "Actividad": "CONT",
                               "Nivel_Educacional": null_attributes('ICG',icg_data.get('NIVEL_EDUCACIONAL', 'NIVEL_EDUCACIONAL')),
                               "Condicion_Laboral": null_attributes('ICG',icg_data.get('CONDICION_LABORAL', 'CONDICION_LABORAL')),
                               "Regimen_Conyugal": 'PART_GAN',
                               "Cargo_PEP": "CARGO_5",
                               "PEP_Relacionado": "OTR",
                               "Es_Vinc_PEP": "N",
                               "Tipo_Actuacion": "I"
                            })
    if relationship.nombre_relacion == 'AUTORIZADOR_ORDENES':
        # if icg__get__Participes(rut) == True:
        #     url = base_url_icg+"/api/persona/ActualizarAutorizadorOrdenes"
        #     method = 'PUT'
        # else:
        url = base_url_icg+"/api/persona/IngresarAutorizadorOrdenes"
        method = 'POST'
        payload = json.dumps({
                              "Tipo_Persona": "TD",
                               "Rut_Participe": rut,
                               "Nacionalidad": null_attributes('ICG',icg_data.get('NACIONALIDAD', 'NACIONALIDAD')),
                               "Fecha_Nacimiento": convert_date(null_attributes('ICG',icg_data.get('FECHA_NACIMIENTO', 'FECHA_NACIMIENTO'))),
                               "Sexo": null_attributes('ICG',icg_data.get('SEXO', 'SEXO')),
                               "Estado_Civil": null_attributes('ICG',icg_data.get('ESTADO_CIVIL', 'ESTADO_CIVIL')),
                               "Rut_Empleador": "",
                               "Nombre_Empleador": "",
                               "Cargo": null_attributes('ICG',icg_data.get('CARGO', 'CARGO')),
                               "Lugar_EnvioCorresp": "P",
                               "Rut_Conyuge": "",
                               "Nombre_1": null_attributes('ICG',icg_data.get('NOMBRE_1', 'NOMBRE_1')),
                               "Nombre_2": null_attributes('ICG',icg_data.get('NOMBRE_2', 'NOMBRE_2')),
                               "Apellido_Paterno": null_attributes('ICG',icg_data.get('APELLIDO_PATERNO', 'APELLIDO_PATERNO')),
                               "Apellido_Materno": null_attributes('ICG',icg_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                               "Pais_Residencia": null_attributes('ICG',icg_data.get('PAIS_RESIDENCIA', 'PAIS_RESIDENCIA')),
                               "Pasaporte": "NA",
                               "Actividad": "CONT",
                               "Nivel_Educacional": null_attributes('ICG',icg_data.get('NIVEL_EDUCACIONAL', 'NIVEL_EDUCACIONAL')),
                               "Condicion_Laboral": null_attributes('ICG',icg_data.get('CONDICION_LABORAL', 'CONDICION_LABORAL')),
                               "Regimen_Conyugal": 'PART_GAN'
                            })
    if relationship.nombre_relacion in ('AUTORIZADOR_ORDENES', 'REPRESENTANTE_LEGAL', 'AGENTE'):
        response = requests.request(method, url, headers=headers, data=payload)
        return response.text
    else:
        return
    

############################## SERVICIOS OPTIMUS ################################
## POST de crear entidades en Optimus
def optimus__post__crearPersonas(entity, rut):
    headers = {'Authorization': 'Bearer '+login_mbi(), 'Content-Type': 'application/json'}
    aux = basecentralizada__get__entity_rut(entity, rut)
    rut = aux[0]
    nombre = aux[1]
    lista = optimus__post__crearPersonas(entity, rut)
    optimus_data = convertir_atributos_basecentralizada_icg('BASE CENTRALIZADA', 'Optimus', lista)
    url = mbi__base_url+"/v1/api/mbi/param/InEnrolamiento"
    method = 'POST'
    payload = json.dumps({
                        'rut':      rut,
                        'nombre':   nombre,
                        'apPaterno':  null_attributes('ICG',optimus_data.get('APELLIDO_PATERNO', 'APELLIDO_PATERNO')),
                        'apMaterno':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'fechaNacimiento':  null_attributes('ICG',optimus_data.get('FECHA_NACIMIENTO', 'FECHA_NACIMIENTO')),
                        'nacionalidad':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'banco':  null_attributes('ICG',optimus_data.get('BANCO', 'BANCO')),
                        'banco2':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'banco3':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'cuentaBanco':  null_attributes('ICG',optimus_data.get('NUMERO_CUENTA_BANCARIA', 'NUMERO_CUENTA_BANCARIA')),
                        'cuentaBanco2':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'cuentaBanco3':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'profesion':  null_attributes('ICG',optimus_data.get('PROFESION', 'PROFESION')),
                        'cargo':  null_attributes('ICG',optimus_data.get('CARGO', 'CARGO')),
                        'rubro':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'nroTelefonoFijo':  null_attributes('ICG',optimus_data.get('TELEFONO', 'APELLIDO_MATERNO')),
                        'nroTelefonoMovil':  null_attributes('ICG',optimus_data.get('TELEFONO', 'TELEFONO')),
                        'relacionPep':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'rutPep':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'nombrePep':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'cargoPep':  null_attributes('ICG',optimus_data.get('CARGO_PEP', 'CARGO_PEP')),
                        'pep':  null_attributes('ICG',optimus_data.get('PEP', 'PEP')),
                        'direccion':  null_attributes('ICG',optimus_data.get('DIRECCION', 'DIRECCION')),
                        'direccionComercial':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'comuna':  null_attributes('ICG',optimus_data.get('DIRECCION_COMUNA', 'DIRECCION_COMUNA')),
                        'ciudad':  null_attributes('ICG',optimus_data.get('DIRECCION_CIUDAD', 'DIRECCION_CIUDAD')),
                        'region':  null_attributes('ICG',optimus_data.get('DIRECCION_REGION', 'DIRECCION_REGION')),
                        'pais':  null_attributes('ICG',optimus_data.get('DIRECCION_PAIS', 'DIRECCION_PAIS')),
                        'comunaComercial':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'ciudadComercial':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'regionComercial':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'paisComercial':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'tipoPerfil':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'reporteComunEstandarCrs':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'numeroIdentidicacionFiscalCrs':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'paisCrs':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'email':  null_attributes('ICG',optimus_data.get('EMAIL', 'EMAIL')),
                        'email2':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'rutConyuge':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'nombreConyuge':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'tipoTrabajador':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'rutEmpleador':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'empleador':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'estadoCivil':  null_attributes('ICG',optimus_data.get('ESTADO_CIVIL', 'ESTADO_CIVIL')),
                        'regimenConyugal':  null_attributes('ICG',optimus_data.get('REGIMEN_CONYUGAL', 'REGIMEN_CONYUGAL')),
                        'fatca':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'identificacionFatca':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'fechaFormulario':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'estadoFormulario':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'imagen':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'urlImg':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'encuesta':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'puntajeEncuesta':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'estadoProceso':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'nivelIngreso':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'origenRecurso':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'vehiculoInversion':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'relacionMBI':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'pstorigen':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'pstusuario':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'numero_direccion':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'rut_relacion_pep':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'nombre_relacion_pep':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'cargo_relacion_pep':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'complemento_direccion':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'complemento_direccion_comercial':  null_attributes('ICG',optimus_data.get('APELLIDO_MATERNO', 'APELLIDO_MATERNO')),
                        'apellido_paterno_conyuge':  "",
                        'apellido_materno_conyuge':  "",
                        'declaracion':  "",
                        'sexo':  null_attributes('ICG',optimus_data.get('SEXO', 'SEXO')),
                        'organizacion':  "",
                        'codigo_ejecutivo':  "",
                        'archivoContrato':  "",
                         'folio': 0
                         })
    response = requests.request(method, url, headers=headers, data=payload)
    return response.text

@app.post("/optimus__post__IngresarOrden", 
         summary="Post de compra por Bolsa usando los credenciales de alguna Corredora", 
         description = "Retorna los atributos de una relacion especifica dada un ID de relacion",
         tags=["Optimus - Compra/Venta de Acciones/CFI"])
def optimus_ingresar_ordenes(documento_identidad, codigo_portafolio, nemotecnico, cantidad,
                             precio, operacion = 'COMPRA', liquidacion = 'T2', vigencia = 'SH',
                             moneda_liquidacion = 'CLP', monto = 0):
    headers = {'Authorization': 'Api-Key y8AJT81e.30JNZMi1ims87IfgReRLevX9b721bXQA',
               'Content-Type': 'application/json'}
    url = 'http://mbipreservicios.optimuscb.cl:9055/api/Ordenes/ingresarorden?access_token=MTpwZGVsYWJhcnJhOjIwMjItMDgtMTIgMTU6MDA6MDUuODQzMzMxMTVhYzYtNzY4NC00Nzk1LTg3YmMtNmIwNzc1NTdiMWM2'
    day = datetime.today().strftime('%Y-%m-%d')
        
    payload = json.dumps({
      "IdEmpresa": 1,
      "IdUsuario": optimus_api_usuario,
      "ClaveUsuario": optimus_api_password,
      "SimularOperacion": "S",
      "Ordenes": [
        {
          "IdInterno": 0,
          "TipoDocumentoIdentidad": "R",
          "DocumentoIdentidad": documento_identidad,
          "CodigoPortafolio": codigo_portafolio,
          "Operacion": operacion,
          "IdTipoOperacion": "COMACN",
          "Instrumento": nemotecnico,
          "PrecioOrden": precio,
          "IdTipoPrecio": "L",
          "IdCondicionDeLiquidacion": liquidacion,
          "DocumentoIdentidadAutorizadoDarOrdenes": documento_identidad,
          "TipoDocumentoIdentidadAutorizadoDarOrdenes": "R",
          "Cantidad": cantidad,
          "IdTipoVigencia": vigencia,
          "FechaVigencia": day,
          "IdMonedaLiquidacion": moneda_liquidacion,
          "IdBolsa": "BCS",
          "PagoCobroCuentaInversion": "",
          "IdFormadePagoCobro": "CIN",
          "IdTipoCanalDistribucion": "RUA",
          "IdTipoCanalIngreso": "CLI",
          "FechaRecepcion": day,
          "TipoCambio": 0,
          "PorcentajeVisibilidad": 1,
          "IdBoveda": "COR",
          "Comentario": "Operacion via API: {}, P = {} - Q = {}".format(nemotecnico, precio, cantidad),
          "IdFormaCustodia": "CUS",
          "Monto": monto
        }
      ],
      "CobrosModificados": [
        {
          "IdInterno": 0,
          "IdCobro": "",
          "ValorCobroAplicado": 0
        }
      ]
    })
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

############################## VISTAS BASE CENTRALIZADA A PLATAFORMA COMERCIAL ################################
@app.get("/basecentralizada__get_atributos_relacion_por_ID", 
         summary="Returns the rut of the entity from Base Centralizada", 
         description = "Retorna los atributos de una relacion especifica dada un ID de relacion",
         tags=["Plataforma Comercial - Relaciones"])
## funcion para crear una conexion con SQL y consultarla
def api_basecentralizada_atributos_relaciones(id_relacion, entity_from, entity_to):
    conn = psycopg2.connect(database="mdm-centralized-base", 
                            user="read_user", 
                            password="FzjhdTKIaXiuiiq", 
                            host="54.86.201.79", 
                            port=5432)
    query_atributosrelaciones = '''
    WITH cliente_documentoidentidad (var1, var2, var3) as (values ({0}, '{1}', '{2}'))

    -- Client-Client --
    select distinct
    	'client'				as entity_from,
    	'client'				as entity_to,
    	rel_client_type.name 	as relacion,
    	rel_client.id			as id_relacion,
    	rel_client_attribute.name as attribute_relationship,
    	rel_client_attribute_value.data as attribute_relationship_value,
    	rel_client_attribute_value.index as attribute_relationship_index	
    from client_clientrelationship rel_client
    left join client_clientrelationshiptype rel_client_type
    	on rel_client_type.id = rel_client.type_id
    left join client_clientclientrelationshipvalue	rel_client_attribute_value
    	on rel_client.id = rel_client_attribute_value.client_relationship_id
    left join client_clientclientrelationshipattribute rel_client_attribute
    	on rel_client_attribute.id = rel_client_attribute_value.client_relationship_attribute_id
    where 	CASE (SELECT var2 FROM cliente_documentoidentidad LIMIT 1) 
    			WHEN 'client' 		THEN 
    									CASE (SELECT var3 FROM cliente_documentoidentidad LIMIT 1)	
    										WHEN 'client' THEN rel_client.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
    									ELSE rel_client.id = 0
    									END
    			ELSE rel_client.id = 0 
    		END

    UNION

    -- Client-Company --
    select distinct
    	'client'				as entity_from,
    	'company'				as entity_to,
    	rel_client_type.name 	as relacion,
    	rel_client.id			as id_relacion,
    	rel_client_attribute.name as attribute_relationship,
    	rel_client_attribute_value.data as attribute_relationship_value,
    	rel_client_attribute_value.index as attribute_relationship_index	
    from company_companyrelationship rel_client
    left join company_companyrelationshiptype rel_client_type
    	on rel_client_type.id = rel_client.type_id
    left join company_companyclientrelationshipvalue	rel_client_attribute_value
    	on rel_client.id = rel_client_attribute_value.company_relationship_id
    left join company_companyclientrelationshipattribute rel_client_attribute
    	on rel_client_attribute.id = rel_client_attribute_value.company_relationship_attribute_id
    where 	CASE (SELECT var2 FROM cliente_documentoidentidad LIMIT 1) 
    			WHEN 'client' 		THEN 
    									CASE (SELECT var3 FROM cliente_documentoidentidad LIMIT 1)	
    										WHEN 'company' THEN rel_client.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
    									ELSE rel_client.id = 0
    									END
    			ELSE rel_client.id = 0 
    		END


    UNION

    -- Client-Portfolio --
    select distinct
    	'client'				as entity_from,
    	'portfolio'				as entity_to,
    	rel_client_type.name 	as relacion,
    	rel_client.id			as id_relacion,
    	rel_client_attribute.name as attribute_relationship,
    	rel_client_attribute_value.data as attribute_relationship_value,
    	rel_client_attribute_value.index as attribute_relationship_index	
    from portfolio_portfolioclientrelationship rel_client
    left join portfolio_portfolioclientrelationshiptype rel_client_type
    	on rel_client_type.id = rel_client.type_id
    left join portfolio_portfolioclientrelationshipvalue	rel_client_attribute_value
    	on rel_client.id = rel_client_attribute_value.portfolio_client_relationship_id
    left join portfolio_portfolioclientrelationshipattribute rel_client_attribute
    	on rel_client_attribute.id = rel_client_attribute_value.portfolio_client_relationship_attribute_id
    where 	CASE (SELECT var2 FROM cliente_documentoidentidad LIMIT 1) 
    			WHEN 'client' 		THEN 
    									CASE (SELECT var3 FROM cliente_documentoidentidad LIMIT 1)	
    										WHEN 'portfolio' THEN rel_client.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
    									ELSE rel_client.id = 0
    									END
    			ELSE rel_client.id = 0 
    		END

    UNION

    -- Company-Portfolio --
    select distinct
    	'company'				as entity_from,
    	'portfolio'				as entity_to,
    	rel_client_type.name 	as relacion,
    	rel_client.id			as id_relacion,
    	rel_client_attribute.name as attribute_relationship,
    	rel_client_attribute_value.data as attribute_relationship_value,
    	rel_client_attribute_value.index as attribute_relationship_index	
    from portfolio_portfoliocompanyrelationship rel_client
    left join portfolio_portfoliocompanyrelationshiptype rel_client_type
    	on rel_client_type.id = rel_client.type_id
    left join portfolio_portfoliocompanyrelationshipvalue	rel_client_attribute_value
    	on rel_client.id = rel_client_attribute_value.portfolio_company_relationship_id
    left join portfolio_portfoliocompanyrelationshipattribute rel_client_attribute
    	on rel_client_attribute.id = rel_client_attribute_value.portfolio_company_relationship_attribute_id
    where 	CASE (SELECT var2 FROM cliente_documentoidentidad LIMIT 1) 
    			WHEN 'company' 	THEN 
    								CASE (SELECT var3 FROM cliente_documentoidentidad LIMIT 1)	
    									WHEN 'portfolio' THEN rel_client.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
    								ELSE rel_client.id = 0
    							END
    			ELSE rel_client.id = 0 
    		END
    		
    UNION
    		
    -- Portfolio-Portfolio --
    select distinct
    	'portfolio'				as entity_from,
    	'portfolio'				as entity_to,
    	rel_client_type.name 	as relacion,
    	rel_client.id			as id_relacion,
    	rel_client_attribute.name as attribute_relationship,
    	rel_client_attribute_value.data as attribute_relationship_value,
    	rel_client_attribute_value.index as attribute_relationship_index	
    from portfolio_portfoliocompanyrelationship rel_client
    left join portfolio_portfoliocompanyrelationshiptype rel_client_type
    	on rel_client_type.id = rel_client.type_id
    left join portfolio_portfolioportfoliorelationshipvalue	rel_client_attribute_value
    	on rel_client.id = rel_client_attribute_value.portfolio_portfolio_relationship_id
    left join portfolio_portfoliocompanyrelationshipattribute rel_client_attribute
    	on rel_client_attribute.id = rel_client_attribute_value.portfolio_portfolio_relationship_attribute_id
    where 	CASE (SELECT var2 FROM cliente_documentoidentidad LIMIT 1) 
    			WHEN 'portfolio' 	THEN 
    									CASE (SELECT var3 FROM cliente_documentoidentidad LIMIT 1)	
    										WHEN 'portfolio' THEN rel_client.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
    									ELSE rel_client.id = 0
    								END
    			ELSE rel_client.id = 0 
    		END
    '''
    consult = pd.read_sql_query(query_atributosrelaciones.format(id_relacion, entity_from, entity_to), conn)
    return JSONResponse(content=consult.to_dict(orient="records"))

@app.get("/basecentralizada__get_relaciones_por_Entidad", 
         summary="Retorna todas las relaciones dado una entidad y su ID", 
         description = "Retorna el RUT de la entidad que se consulta a nivel de ID para Persona Natural o Juridica y el REFERENCE_ID"+
         "para portfolios",
         dependencies=[Depends(jwtBearer())],
         tags=["Plataforma Comercial - Relaciones"])
## funcion para crear una conexion con SQL y consultarla
def api_basecentralizada_relaciones_porID(id_relacion, entity_from):
    conn = psycopg2.connect(database=basecentralizada_db_database, 
                            user=basecentralizada_db_user, 
                            password=basecentralizada_db_password, 
                            host=basecentralizada_db_server, 
                            port=basecentralizada_db_port)
    query_relaciones_por_ID = '''
        WITH cliente_documentoidentidad (var1, var2) as (values ({0}, '{1}'))
        
        -- Client-Client --
        select distinct
        	client_id_from.id 		as id_client_from,
        	client_id_from.name 	as name_client_from,
        	client_id_from.rut 		as identifier_client_from,
        	'client'				as entity_from,
        	client_id_to.id 		as id_client_to,
        	client_id_to.name 		as name_client_to,
        	client_id_to.rut 		as identifier_client_to,
        	'client'				as entity_to,
        	rel_client_type.name 	as relacion,
        	rel_client.id			as id_relacion
        --	rel_client_attribute.name as attribute_relationship,
        --	rel_client_attribute_value.data as attribute_relationship_value,
        --	rel_client_attribute_value.index as attribute_relationship_index	
        from client_clientrelationship rel_client
        left join client_client client_id_from
        	on client_id_from.id = rel_client.client_from_id
        left join client_clientrelationshiptype rel_client_type
        	on rel_client_type.id = rel_client.type_id
        left join client_client client_id_to
        	on client_id_to.id = rel_client.client_to_id
        --left join client_clientclientrelationshipvalue	rel_client_attribute_value
        --	on rel_client.id = rel_client_attribute_value.client_relationship_id
        --left join client_clientclientrelationshipattribute rel_client_attribute
        --	on rel_client_attribute.id = rel_client_attribute_value.client_relationship_attribute_id
        where 	CASE (SELECT var2 FROM cliente_documentoidentidad LIMIT 1)
        			WHEN 'client' 		THEN client_id_from.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
        			WHEN 'company' 		THEN client_id_from.id = 0
        			WHEN 'portfolio' 	THEN client_id_from.id = 0
        		END
        
        UNION
        
        -- Client - Company --
        select distinct
        	client_id_from.id 		as id_client_from,
        	client_id_from.name 	as name_client_from,
        	client_id_from.rut 		as identifier_client_from,
        	'client'				as entity_from,
        	client_id_to.id 		as id_client_to,
        	client_id_to.name 		as name_client_to,
        	client_id_to.rut 		as identifier_client_to,
        	'company'				as entity_to,
        	rel_client_type.name 	as relacion,
        	rel_client.id			as id_relacion
        --	rel_client_attribute.name as attribute_relationship,
        --	rel_client_attribute_value.data as attribute_relationship_value,
        --	rel_client_attribute_value.index as attribute_relationship_index	
        from company_companyrelationship rel_client
        left join client_client client_id_from
        	on client_id_from.id = rel_client.client_from_id
        left join company_companyrelationshiptype rel_client_type
        	on rel_client_type.id = rel_client.type_id
        left join company_company client_id_to
        	on client_id_to.id = rel_client.company_to_id
        --left join company_companyclientrelationshipvalue rel_client_attribute_value
        --	on rel_client.id = rel_client_attribute_value.company_relationship_id
        --left join company_companyclientrelationshipattribute rel_client_attribute
        --	on rel_client_attribute.id = rel_client_attribute_value.company_relationship_attribute_id
        where 	CASE (SELECT var2 FROM cliente_documentoidentidad LIMIT 1)
        			WHEN 'client' 		THEN client_id_from.id = 0
        			WHEN 'company' 		THEN client_id_to.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
        			WHEN 'portfolio' 	THEN client_id_from.id = 0
        		END
        --		and rel_client_type.name = (SELECT var2 FROM cliente_documentoidentidad LIMIT 1)
        
        UNION
        
        -- Client - Portfolios --
        select distinct
        	client_id_from.id 		as id_client_from,
        	client_id_from.name 	as name_client_from,
        	client_id_from.rut 		as identifier_client_from,
        	'client'				as entity_from,
        	client_id_to.id 		as id_client_to,
        	client_id_to.reference_id 		as name_client_to,
        	client_id_to.reference_id 		as identifier_client_to,
        	'portfolio'				as entity_to,
        	rel_client_type.name 	as relacion,
        	rel_client.id			as id_relacion
        --	rel_client_attribute.name as attribute_relationship,
        --	rel_client_attribute_value.data as attribute_relationship_value,
        --	rel_client_attribute_value.index as attribute_relationship_index	
        from portfolio_portfolioclientrelationship rel_client
        left join client_client client_id_from
        	on client_id_from.id = rel_client.client_from_id
        left join portfolio_portfolioclientrelationshiptype rel_client_type
        	on rel_client_type.id = rel_client.type_id
        left join portfolio_portfolio client_id_to
        	on client_id_to.id = rel_client.portfolio_to_id
        --left join client_clientclientrelationshipvalue	rel_client_attribute_value
        --	on rel_client.id = rel_client_attribute_value.client_relationship_id
        --left join client_clientclientrelationshipattribute rel_client_attribute
        --	on rel_client_attribute.id = rel_client_attribute_value.client_relationship_attribute_id
        where 	CASE (SELECT var2 FROM cliente_documentoidentidad LIMIT 1)
        			WHEN 'client' 		THEN client_id_from.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
        			WHEN 'company' 		THEN client_id_to.id = 0
        			WHEN 'portfolio' 	THEN client_id_to.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
        		END
        
        UNION
        
        -- Company-Portfolio --
        select distinct
        	client_id_from.id 		as id_client_from,
        	client_id_from.name 	as name_client_from,
        	client_id_from.rut 		as identifier_client_from,
        	'company'				as entity_from,
        	client_id_to.id 		as id_client_to,
        	client_id_to.reference_id 		as name_client_to,
        	client_id_to.reference_id 		as identifier_client_to,
        	'portfolio'				as entity_to,
        	rel_client_type.name 	as relacion,
        	rel_client.id			as id_relacion
        --	rel_client_attribute.name as attribute_relationship,
        --	rel_client_attribute_value.data as attribute_relationship_value,
        --	rel_client_attribute_value.index as attribute_relationship_index	
        from portfolio_portfoliocompanyrelationship rel_client
        left join company_company client_id_from
        	on client_id_from.id = rel_client.company_from_id
        left join portfolio_portfolioclientrelationshiptype rel_client_type
        	on rel_client_type.id = rel_client.type_id
        left join portfolio_portfolio client_id_to
        	on client_id_to.id = rel_client.portfolio_to_id
        --left join client_clientclientrelationshipvalue	rel_client_attribute_value
        --	on rel_client.id = rel_client_attribute_value.client_relationship_id
        --left join client_clientclientrelationshipattribute rel_client_attribute
        --	on rel_client_attribute.id = rel_client_attribute_value.client_relationship_attribute_id
        where 	CASE (SELECT var2 FROM cliente_documentoidentidad LIMIT 1)
        			WHEN 'client' 		THEN client_id_from.id = 0
        			WHEN 'company' 		THEN client_id_to.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
        			WHEN 'portfolio' 	THEN client_id_to.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
        		END
        
        UNION 
        
        -- Portfolio-Portfolio --
        select distinct
        	client_id_from.id 		as id_client_from,
        	client_id_from.reference_id 	as name_client_from,
        	client_id_from.reference_id 		as identifier_client_from,
        	'portfolio'				as entity_from,
        	client_id_to.id 		as id_client_to,
        	client_id_to.reference_id 		as name_client_to,
        	client_id_to.reference_id 		as identifier_client_to,
        	'portfolio'				as entity_to,
        	rel_client_type.name 	as relacion,
        	rel_client.id			as id_relacion
        --	rel_client_attribute.name as attribute_relationship,
        --	rel_client_attribute_value.data as attribute_relationship_value,
        --	rel_client_attribute_value.index as attribute_relationship_index	
        from portfolio_portfoliorelationship rel_client
        left join portfolio_portfolio client_id_from
        	on client_id_from.id = rel_client.portfolio_from_id
        left join portfolio_portfoliorelationshiptype rel_client_type
        	on rel_client_type.id = rel_client.type_id
        left join portfolio_portfolio client_id_to
        	on client_id_to.id = rel_client.portfolio_to_id
        --left join client_clientclientrelationshipvalue	rel_client_attribute_value
        --	on rel_client.id = rel_client_attribute_value.client_relationship_id
        --left join client_clientclientrelationshipattribute rel_client_attribute
        --	on rel_client_attribute.id = rel_client_attribute_value.client_relationship_attribute_id
        where 	CASE (SELECT var2 FROM cliente_documentoidentidad LIMIT 1)
        			WHEN 'client' 		THEN client_id_from.id = 0
        			WHEN 'company' 		THEN client_id_to.id = 0
        			WHEN 'portfolio' 	THEN client_id_from.id = (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
        		END
    '''
    consult = pd.read_sql_query(query_relaciones_por_ID.format(id_relacion, entity_from), conn)
    return JSONResponse(content=consult.to_dict(orient="records"))

@app.get("/icg__get__instrumentos_agente_encustodia", 
         summary="Retorna todas las relaciones dado una entidad y su ID", 
         description = "Retorna el RUT de la entidad que se consulta a nivel de ID para Persona Natural o Juridica y el REFERENCE_ID"+
         "para portfolios",
         dependencies=[Depends(jwtBearer())],
         tags=["ICG"])
## funcion para crear una conexion con SQL y consultarla
def icg__get__instrumentos_agente_encustodia(rut):
    conn = pyodbc.connect('Driver={'+driver_sql+'};'
                          'Server='+icg_db_server+';'
                          'Database='+icg_db_database+';'
                          'UID='+icg_db_user+';'
                          'PWD='+icg_db_password+'')
    query = '''
            exec sp_platcom_instrumentos_agente_encustodia '{0}'
        '''
    consult = pd.read_sql_query(query.format(rut), conn)
    return JSONResponse(content=consult.to_dict(orient="records"))

@app.get("/sp_platcom_get_all_fondos_ruts_x_agente", 
         summary="Retorna todos los portfolio a nivel de Caja y Patrimonio para un agente", 
         description = "Informacion de los portafolios asignados a un agente a nivel de ICG",
         dependencies=[Depends(jwtBearer())],
         tags=["Plataforma Comercial - Vista Resumen Agente"])
## funcion para crear una conexion con SQL y consultarla
def sp_platcom_get_all_fondos_ruts_x_agente(rut):
    conn = pyodbc.connect('Driver={'+driver_sql+'};'
                          'Server=192.168.0.159;'
                          'Database=BDTransformacionDigitalMBI;'
                          'UID='+icg_db_user+';'
                          'PWD='+icg_db_password+'')
    query = '''
            exec [dbo].[sp_platcom_get_all_fondos_ruts_x_agente] '{0}'
        '''
    consult = pd.read_sql_query(query.format(rut), conn)
    return JSONResponse(content=consult.to_dict(orient="records"))

@app.get("/basecentralizada_get_portfolios_x_agente", 
         summary="Retorna todos los portfolios asociados a un agente para un Agente desde Base Centralizada", 
         description = "Relaciones entre clientes y agentes desde Base Centralizada",
         dependencies=[Depends(jwtBearer())],
         tags=["Plataforma Comercial - Vista Resumen Agente"])
## funcion para crear una conexion con SQL y consultarla
def basecentralizada_get_portfolios_x_agente(rut):
    conn = psycopg2.connect(database=basecentralizada_db_database, 
                            user=basecentralizada_db_user, 
                            password=basecentralizada_db_password, 
                            host=basecentralizada_db_server, 
                            port=basecentralizada_db_port)
    query = '''
        WITH cliente_documentoidentidad (var1, var2, var3) as (values ('{0}', 'AGENTE', 'DUEÑO'))
           
           -- Personas Naturales --
           select  distinct
                   client_id_from.id           as id_from,
                   client_id_from.name         as nombre_from,
                   client_id_from.rut          as rut_from,
                   client_id_to.id             as id_client_to,
                   client_id_to.name           as nombre_to,
                   rel_client_type.name        as relacion,
                   client_id_to.rut            as rut_to,
                   porta.reference_id          as portafolio,
                   tomador.rut                 as rut_tomador,
                   tomador.name                as nombre_tomador
           from client_clientrelationship rel_client
           left join client_client client_id_from
               on client_id_from.id = rel_client.client_from_id
           left join client_clientrelationshiptype rel_client_type
               on rel_client_type.id = rel_client.type_id
           left join client_client client_id_to
               on client_id_to.id = rel_client.client_to_id
           left join portfolio_portfolioclientrelationship porta_rel
               on porta_rel.client_from_id = client_id_to.id
           left join portfolio_portfolioclientrelationshiptype porta_rel_tipo
               on porta_rel_tipo.id = porta_rel.type_id
           left join portfolio_portfolio porta
               on porta_rel.portfolio_to_id = porta.id
           left join portfolio_portfolioclientrelationshiptype porta_rel_tipo2
               on porta_rel_tipo2.name = 'TOMADOR_DECISIONES'
           left join portfolio_portfolioclientrelationship porta_rel2
               on porta_rel2.portfolio_to_id = porta.id
               and porta_rel_tipo2.id = porta_rel2.type_id
           left join client_client tomador
               on tomador.id = porta_rel2.client_from_id
           where   client_id_from.rut          =   (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
                   and rel_client_type.name    =   (SELECT var2 FROM cliente_documentoidentidad LIMIT 1)
                   and porta_rel_tipo.name     =   (SELECT var3 FROM cliente_documentoidentidad LIMIT 1)

           UNION

           -- Personas Jurídicas --
           select  distinct
                   client_id_from.id           as id_from,
                   client_id_from.name         as nombre_from,
                   client_id_from.rut          as rut_from,
                   company_to.id               as id_client_to,
                   company_to.name             as nombre_to,
                   rel_company_type.name       as nombre_to,
                   company_to.rut              as rut_to,
                   porta.reference_id          as portafolio,
                   tomador.rut                 as rut_tomador,
                   tomador.name                as nombre_tomador
           from company_companyrelationship rel_company
           left join client_client client_id_from
               on client_id_from.id = rel_company.client_from_id
           left join company_companyrelationshiptype rel_company_type
               on rel_company_type.id = rel_company.type_id
           left join company_company company_to
               on rel_company.company_to_id = company_to.id
           left join portfolio_portfoliocompanyrelationship porta_rel
               on porta_rel.company_from_id = company_to.id
           left join portfolio_portfoliocompanyrelationshiptype porta_rel_tipo
               on porta_rel_tipo.id = porta_rel.type_id
           left join portfolio_portfolio porta
               on porta_rel.portfolio_to_id = porta.id
           left join portfolio_portfoliocompanyrelationshiptype porta_rel_tipo2
               on porta_rel_tipo2.name = 'TOMADOR_DECISIONES'
           left join portfolio_portfolioclientrelationship porta_rel2
               on porta_rel2.portfolio_to_id = porta.id
               and porta_rel_tipo2.id = porta_rel2.type_id
           left join client_client tomador
               on tomador.id = porta_rel2.client_from_id
           where   client_id_from.rut          =   (SELECT var1 FROM cliente_documentoidentidad LIMIT 1)
                   and rel_company_type.name   =   (SELECT var2 FROM cliente_documentoidentidad LIMIT 1)
                   and porta_rel_tipo.name     =   (SELECT var3 FROM cliente_documentoidentidad LIMIT 1)

           order by rut_from
        '''
    consult = pd.read_sql_query(query.format(rut), conn)
    return JSONResponse(content=consult.to_dict(orient="records"))



############################## VISTAS BASE CENTRALIZADA A PLATAFORMA COMERCIAL ################################

# a = sp_platcom_get_all_fondos_ruts_x_agente('13235032-9')
# print(a)
# print(a.content)
# print(a.head())