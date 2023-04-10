"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #
    nombres= ["cluster","cantidad_de_palabras_clave","porcentaje_de_palabras_clave","principales_palabras_clave"]
    df = pd.read_fwf("clusters_report.txt",names = nombres,skiprows=4)

    df["cluster"].fillna(method="ffill",inplace=True)
    df["cantidad_de_palabras_clave"].fillna(method="ffill",inplace=True)
    df["porcentaje_de_palabras_clave"].fillna(method="ffill",inplace=True)
    df = df.groupby(["cluster","cantidad_de_palabras_clave","porcentaje_de_palabras_clave"])["principales_palabras_clave"].apply(" ".join).reset_index()    
    df["porcentaje_de_palabras_clave"].replace(" %","", regex=True, inplace=True)
    df["porcentaje_de_palabras_clave"].replace(",",".", regex=True, inplace=True)
    df["porcentaje_de_palabras_clave"]=df["porcentaje_de_palabras_clave"].astype(float)
    df["principales_palabras_clave"]=df["principales_palabras_clave"].map(lambda x:x.replace(".",""))
    df['principales_palabras_clave']=df['principales_palabras_clave'].replace('\s+', ' ', regex=True)
    #df["porcentaje_de_palabras_clave"].replace("","%",inplace=True)    
    return df





