# Databricks notebook source
# MAGIC %md
# MAGIC #I. Inicializacion

# COMMAND ----------

# MAGIC %md
# MAGIC ##A. Inicializacion

import yfinance as yf
import pandas as pd
import re

# COMMAND ----------

def extractDatafromYahoofinance(ticker, start_date = '2020-1-1', end_date = '2024-4-30', resampling_per = 'M'):
  """
  Funcion para extraer datos desde yahoo finance
  """
  data = yf.Ticker(ticker) 
  df = data.history(period='1mo', start=start_date, end=end_date)
  data_resample = df.resample(resampling_per).last().reset_index()
  data_out = data_resample[['Date', 'Open']].rename(columns={'Open': f'{ticker}_Open'})
  return data_out

def mergeMultipleDataframe(df_list, column):
  """
  Funcion para unir dataframes para posterior ingesta en delta lake
  """
  df_final = df_list[0]
  for df in df_list[1:]:
      df_final = df_final.merge(df, on=column, how='outer')    
  return df_final

def unify_date_column(df_list):
  """
  Funcion para unificar zonas horarias entre distintos datos extraidos y convertir a periodo_ref
  """
  for df in df_list:
      df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None) # cambiar zona horaria
      df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y%m').astype(int) # dejar en formato periodo_ref 
  return df_list

def clean_column_names(df):
  """
  Limpiar nombre de columnas para guardar en delta
  """
  df_cleaned = df.copy()
  for col in df.columns:
      new_col = re.sub(r"[ ,;{}()\n\t=]", "_", col)  # Replaza los caracteres no válidos con "_"
      df_cleaned.rename(columns={col: new_col}, inplace=True)
  return df_cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ##B. Variables

# COMMAND ----------

# MAGIC %md
# MAGIC ###i. De mercado

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. ETF y acciones

# COMMAND ----------

data_list = list()

for i in ["ECH", "ACWI", "SPY", "EEM", "ILF", "EMB", "CEMB", "LEMB"]:
  data = extractDatafromYahoofinance(i)
  data_list.append(data)

print(len(data_list))

# COMMAND ----------

for i in ["USDCLP=X"]:
  data = extractDatafromYahoofinance(i)
  data_list.append(data)

print(len(data_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Tasas

# COMMAND ----------

# Tasas
for i in ["FED", "^TNX"]:
  try:
    data = extractDatafromYahoofinance(i)
    data_list.append(data)
  except:
    pass

print(len(data_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Indicadores e índices

# COMMAND ----------

# Tasas
for i in ["^VIX", "HG=F", "BTC-USD"]:
  try:
    data = extractDatafromYahoofinance(i)
    data_list.append(data)
  except:
    pass

print(len(data_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ##C. Creacion en el Delta

# COMMAND ----------

df_list = unify_date_column(data_list)
df = mergeMultipleDataframe(df_list, 'Date')
df.head()

# COMMAND ----------

df_cleaned = clean_column_names(df)
print(df_cleaned.head())
df_cleaned.to_csv()