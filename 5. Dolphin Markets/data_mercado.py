#! C:\Users\pdelabarra\Documents\Proyectos_Aplicaciones\5. Dolphin Markets\env\Scripts\python.exe

import pandas as pd
import requests
import matplotlib.pyplot as plt
import matplotlib
import pyodbc
import seaborn as sns

matplotlib.use('TkAgg')
api_key = "418e3aaf-7e3a-4a66-a14b-5bcd25871984"

## funcion para crear una conexion con SQL y consultarla
def sql_connect(server, database, user, password, consulta):
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server='+server+';'
                          'Database='+database+';'
                          'UID='+user+';'
                          'PWD='+password+'')
    consult = pd.read_sql_query(consulta, conn)
    return consult

## Listado de las compañías disponbiles
def dolphinmarkets_listado_company(type):
    payload = {
                "key": api_key,
                "companies": "?",
                "type": type,
                }
    r = requests.post('https://dolphin.markets/api/financial-statements', json=payload)
    df = pd.DataFrame(r.json())
    return df

## Data de los fondos de inversión en valores cuota
def dolphinmarkets_fondos_data(ruts = ['9637', '9572', '9621']):
    payload = {
                "key": api_key,
                'ruts': ruts
                }
    r = requests.post('https://dolphin.markets/api/funds', json=payload)
    df = pd.DataFrame(r.json())
    return df

## Data de los precios de las acciones
def dolphinmarkets_acciones_data(tickers, data_points = ["close"], start_date ="2018-01-01"):
    payload = {
                "key": api_key,
                'tickers': tickers,
                'values': data_points,
                'start': start_date,
                }
    r = requests.post('https://dolphin.markets/api/stocks', json=payload)
    df = pd.DataFrame(r.json())
    return df

## Data de los estados financieros de las empresas - EFE / ESF / EERR
def dolphinmarkets_acciones_eeff(companies, type ="ESF"):
    payload = {
                "key": api_key,
                'companies': companies,
                'type': type
                }
    r = requests.post('https://dolphin.markets/api/financial-statements', json=payload)
    df = pd.DataFrame(r.json())
    return df

## Data de los ratios financieros a consultar
def dolphinmarkets_acciones_ratios(companies, ratios):
    payload = {
                "key": api_key,
                'companies': companies,
                'ratios': ratios,
                }
    r = requests.post('https://dolphin.markets/api/ratios', json=payload)
    df = pd.DataFrame(r.json())
    return df

estados = dolphinmarkets_acciones_eeff(['CAP S.A.',	'CELULOSA ARAUCO Y CONSTITUCION S.A.',	
                                         'CEMENTO POLPAICO S.A.',	'EMPRESAS CMPC S.A.',
                                         'CELULOSA ARAUCO Y CONSTITUCION S.A.',	
                                         'SOCIEDAD DE INVERSIONES ORO BLANCO S.A.',	
                                         'SOCIEDAD DE INVERSIONES PAMPA CALICHERA S.A.',	
                                         'SOCIEDAD QUIMICA Y MINERA DE CHILE S.A.'],
                                         "EERR")
estados = estados[estados["ifrs"] == "Profit Loss"]
estados["date"] = pd.to_datetime(estados['date'])
estados = estados[estados["date"].dt.year > 2012 ]
estados["date_analysis"] = estados["date"] - pd.offsets.DateOffset(years=1)
precios = dolphinmarkets_acciones_data(["CAP", "ARAUCO", "POLPAICO", "CMPC", "ORO BLANCO", "CALICHERA", "SQM-B", "PAMPA"], 
                                       ["close"], "2012-01-01")
precios["date_price"] = pd.to_datetime(precios['date'])
shares = pd.read_excel("Shares.xlsx")
shares["date_stocks"] = pd.to_datetime(shares['Date'])
instrumentos = {
                "ticker": [ 'CAP',
                            'ARAUCO',
                            'POLPAICO',
                            'CMPC',
                            'ORO BLANCO',
                            'PAMPA',
                            'SQM-B'],
                "name":
                        [   'CAP S.A.',
                            'CELULOSA ARAUCO Y CONSTITUCION S.A.',
                            'CEMENTO POLPAICO S.A.',
                            'EMPRESAS CMPC S.A.',
                            'SOCIEDAD DE INVERSIONES ORO BLANCO S.A.',
                            'SOCIEDAD DE INVERSIONES PAMPA CALICHERA S.A.',
                            'SOCIEDAD QUIMICA Y MINERA DE CHILE S.A.']
                }
instrumentos = pd.DataFrame(instrumentos)

print(shares.head(5))
print(estados.head(5))
print(estados.head(5))

# merged_df = pd.merge(estados, instrumentos, left_on='company',
#                      right_on="name", how='left')
# merged_df = pd.merge(merged_df, shares, left_on=['ticker', 'date'],
#                      right_on=["Ticker", "date_stocks"], how='left')

# merged_df = merged_df.fillna(method='ffill')

# merged_df["EPS"] = merged_df["unit"] / merged_df["Amount"]
# merged_df_tr = pd.merge(merged_df, precios, left_on=['ticker', 'date'],
#                      right_on=["ticker", "date_price"], how='left')
# merged_df_fw = pd.merge(merged_df, precios, left_on=['ticker', 'date_analysis'],
#                      right_on=["ticker", "date_price"], how='left')
# merged_df_tr['P/E'] = merged_df_tr['EPS'].fillna(method='ffill')
# merged_df_tr["P/E"] = merged_df_tr['EPS'] / merged_df_tr['unit']
# sns.lineplot(x='date_x', y='P/E', hue='company', data = merged_df_tr)
# # set title and axis labels
# plt.title('P/E Trailing')
# plt.xlabel('Date')
# plt.ylabel('P/E')
# # show plot
# plt.show()
# print(merged_df.head(10))

# merged_df_fw['P/E'] = merged_df_fw['EPS'].fillna(method='ffill')
# merged_df_fw["P/E"] = merged_df_fw['EPS'] / merged_df_fw['unit']
# sns.lineplot(x='date_x', y='P/E', hue='company', data = merged_df_fw)
# # set title and axis labels
# plt.title('P/E Forward')
# plt.xlabel('Date')
# plt.ylabel('P/E')
# # show plot
# plt.show()
# print(merged_df.head(10))

ratios = ['ebitda',
'ebitda_margin',
'net_debt',
'net_debt_to_ebitda',
'net_debt_to_equity',
'efficiency',
'nim',
'nim_margin',
'leverage',
'net_margin',
'roa',
'roe',
'net_financial_expenses',
'roic',
'ebitda_to_net_financial_expenses',
'inventory_turnover']

empresas = ['CAP S.A.',	'CELULOSA ARAUCO Y CONSTITUCION S.A.',	
                                         'CEMENTO POLPAICO S.A.',	'EMPRESAS CMPC S.A.',
                                         'CELULOSA ARAUCO Y CONSTITUCION S.A.',	
                                         'SOCIEDAD DE INVERSIONES ORO BLANCO S.A.',	
                                         'SOCIEDAD DE INVERSIONES PAMPA CALICHERA S.A.',	
                                         'SOCIEDAD QUIMICA Y MINERA DE CHILE S.A.']


df = dolphinmarkets_acciones_ratios(empresas, ratios)
print(df.columns)
df['date'] = pd.to_datetime(df['date'])

# Extract unique values in account column
ratios = df['account'].unique()

# Loop over each ratio and create a plot
for ratio in ratios:
    df_ratio = df.query('account == @ratio')
    sns.set_style("whitegrid")
    sns.lineplot(data=df_ratio, x='date', y='unit', hue='company', style='company', markers=True)
    plt.title(ratio)
    plt.show()