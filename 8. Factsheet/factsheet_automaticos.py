import requests

# Reemplaza los siguientes valores con los datos de tu entorno de Power BI
client_id = '2278703c-200f-4f72-a712-a773cef57138'
client_secret = 'HU68Q~bt9a7sAVuJMY0pDBiVANhEp8AeqJLKJbBU'
tenant_id = '42d93a3f-7872-4889-b595-f1a0e3122ebf'
group_id = 'me'
report_id = '56b855be-d438-4a10-9ca9-b68a95bbb4e4'
page_id = 'ReportSectionadaa41941ee653b5d390'

# Autenticación
url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
payload = {
    'grant_type': 'password',
    'client_id': client_id,
    'client_secret': "HU68Q~bt9a7sAVuJMY0pDBiVANhEp8AeqJLKJbBU",
    'resource': 'https://analysis.windows.net/powerbi/api',
    'scope': 'openid',
    'username': 'benjamin.rodriguez@mbi.cl',
    'password': 'brodriguez2021..'
}

response = requests.post(url, data=payload)
print(response.json())
access_token = response.json()['access_token']

# Consultar valores únicos en la columna Nombre de la tabla Clientes
dax_query = '''
EVALUATE
SUMMARIZECOLUMNS(
    'Fondos'[Codigo_Fdo]
)
'''

data_url = f'https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports/{report_id}/data'
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}
body = {
    'version': '1.0',
    'queries': [
        {
            'query': dax_query,
            'queryId': 'GetUniqueValues'
        }
    ]
}

data_response = requests.post(data_url, headers=headers, json=body)
unique_values = [row[0] for row in data_response.json()['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']]

# Exportar informe en formato PDF
export_url = f'https://api.powerbi.com/v1.0/myorg/groups/{group_id}/reports/{report_id}/ExportTo'

# Exportar reporte en formato PDF
for value in unique_values:
    body = {
        'format': 'PDF',
        'powerBIReportConfiguration': {
            'pages': [
                {
                    'name': page_id,
                    'filters': [
                        {
                            'target': {
                                'table': 'Fondos',
                                'column': 'Codigo_Fdo'
                            },
                            'operator': 'eq',
                            'values': [value]
                        },
                        {
                            'target': {
                                'table': 'Fondos',
                                'column': 'Fecha'
                            },
                            'operator': 'eq',
                            'values': ["31-03-2023"]
                        },
                        {
                            'target': {
                                'table': 'Fondos',
                                'column': 'Series'
                            },
                            'operator': 'eq',
                            'values': ["U"]
                        }
                    ]
                }
            ]
        }
    }

    export_response = requests.post(export_url, headers=headers, json=body)

    # Guardar el archivo PDF con el nombre basado en el valor del filtro
    with open(f'reporte_{value}.pdf', 'wb') as file:
        file.write(export_response.content)
