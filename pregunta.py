'''
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.

'''


import pandas as pd
import re

def ingest_data():
    df=pd.read_fwf('clusters_report.txt', colspecs='infer',widths=[9,16,16,80],header=None,
                   names=['cluster','cantidad_de_palabras_clave','porcentaje_de_palabras_clave','principales_palabras_claves'],
                   converters={'porcentaje_de_palabras_clave':lambda x:x.rstrip(' %').replace(',','.')}).drop(index={0,1,2}).ffill()
    #df=df.iloc[3:]
    df.columns=df.columns.str.lower()
    df.columns=df.columns.str.replace(' ','_')
    # Reemplazar los espacios en blanco en las palabras clave con un solo espacio
    df['principales_palabras_claves'] = df['principales_palabras_claves'].str.replace(r'\s+', ' ')

    # Convertir las palabras clave en una lista de palabras separadas por comas y un solo espacio entre cada palabra
    df['principales_palabras_claves'] = df['principales_palabras_claves'].str.split(', ')

    # Mostrar el DataFrame sin indexado adicional
    print(df.to_string(index=False))

    # Mostrar el DataFrame
    print(df)
    return df

# Llamar a la función para ingestar los datos
ingest_data()


