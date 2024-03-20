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

    #
    # Inserte su código aquí
    #

    # Leer el archivo con pd.read_fwf()
    df = pd.read_fwf("clusters_report.txt", # Este método se utiliza para leer archivos de texto con campos de ancho fijo
                    # Pandas infiere automáticamente las posiciones de las columnas basándose en los datos presentes en el archivo.
                    colspecs="infer", 
                    # especifica la anchura de cada columna en el archivo
                    widths=[9, 16, 16, 80], 
                    # No hay una fila con encabezado
                    header=None, 
                    # # Nombres de cada columa
                    names=["cluster", "cantidad_de_palabras_clave", "porcentaje_de_palabras_clave", "principales_palabras_clave"], 
                    # la columna "porcentaje_de_palabras_clave" se convierte eliminando el signo de porcentaje (%) al final de cada valor 
                    # y reemplazando las comas (",") con puntos (".") para permitir la conversión a un tipo numérico posteriormente.
                    converters={"porcentaje_de_palabras_clave": lambda x: x.rstrip(" %").replace(",", ".")}
                    ).drop(index={0,1,2}).ffill()
    # Convertir los nombres de las columnas a minúsculas
    df.columns = df.columns.str.lower()

    # Reemplazar espacios por guiones bajos en los nombres de las columnas
    df.columns = df.columns.str.replace(' ', '_')

    # astype(), está pasando un diccionario donde las claves son los nombres de las columnas
    # y los valores son los nuevos tipos de datos para las columnas.
    df = df.astype  ({ "cluster": int, 
                    "cantidad_de_palabras_clave": int, 
                    "porcentaje_de_palabras_clave": float,
                    "principales_palabras_clave": str
                    })

    # Agrupa por la tres columnas y principales palabra clave la convierte en una cadena separada por espacio
    df = df.groupby(["cluster","cantidad_de_palabras_clave","porcentaje_de_palabras_clave"])["principales_palabras_clave"].apply(lambda x: ' '.join(map(str,x))).reset_index()
    
    # la columna "porcentaje_de_palabras_clave" redondea valores a 1 decimal.
    df["porcentaje_de_palabras_clave"] = df["porcentaje_de_palabras_clave"].apply(lambda x: round(x,1))
    
    # elimina los espacios en blanco adicionales y los puntos al final de cada cadena en la columna "principales_palabras_clave"
    df["principales_palabras_clave"] = df["principales_palabras_clave"].apply(lambda x: re.sub(r'\s+', ' ',x).rstrip("."))
    print(df)
  
    return df

# Llamar a la función para ingestar los datos
ingest_data()


