"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re

def ingest_data():
    df=pd.read_fwf('clusters_report.txt', colspecs='infer',widths=[9,16,16,80],headre=None,
                   names=['cluster','cantidad_de_palabras_clave','porcentaje_de_palabras_clave','principales_palabras_claves'],
                   converters={'porcentaje_de_palabras_clave':lambda x:x.rstrip(' %').replace(',','.')}).drop(index={0,1,2}).ffill()
    df.columns=df.columns.str.lower()
    df.columns=df.columns.str.replace(' ','_')
    

    # Mostrar el DataFrame
    print(df)
    return df

# Llamar a la función para ingestar los datos
ingest_data()

