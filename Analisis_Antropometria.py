from datetime import timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from airflow import DAG
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging

default_args = {
    'owner': 'Diego',
    'depends_on_past': False,
    'email': ['diegoroman199@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5)
}

def extraer():
    df = pd.read_csv("https://kf.kobotoolbox.org/api/v2/assets/a3MRsgwdgcjD5c2qKzdNS4/export-settings/esSPAby29U5Tbhyr63iLZft/data.csv", sep=";")
    df.to_csv("/tmp/antropometria.csv", index=False
               )

def trasnformar():
    anthro = importr('anthro')
    data = pd.read_csv("/tmp/antropometria.csv")
    data.fillna(value=" ")
    pandas2ri.activate()
    r_data = pandas2ri.py2rpy(data)
    sex = r_data .rx2['sexo_num']
    age = r_data .rx2['edad_dias']
    weight = r_data .rx2['Ingrese el peso']
    lenhei = r_data .rx2['Ingrese la talla']
    headc = r_data .rx2['Circunferencia cefalica']
    armc = r_data .rx2['Circunferencia braquial']
    oedema = r_data .rx2['oedema']
    newdata = anthro.anthro_zscores(sex=sex, age=age, weight=weight, lenhei=lenhei, headc=headc, armc=armc, oedema=oedema)
    newdata_df = pandas2ri.rpy2py_dataframe(newdata)
    newdata_df.reset_index(drop=True, inplace=True)
    data.reset_index(drop=True, inplace=True)

    new_columns = {
    'Talla_edad': newdata_df["zlen"],
    'IMC': newdata_df["zbmi"],
    'peso_edad': newdata_df["zwei"],
    'circunferencia_cefalica_edad': newdata_df["zhc"],
    'perimetro_braquial_edad': newdata_df["zac"],
    'peso_talla': newdata_df["zwfl"],
    # Agrega aquí las columnas adicionales que deseas incluir
    }

    # Crear un nuevo dataframe con las columnas seleccionadas y los nombres modificados
    new_df = pd.DataFrame(new_columns)
    #new_df["_index"] = data["_index"]

    merged_df = pd.concat([data,new_df], axis=1)
    merged_df.to_csv("/tmp/antropometria_indi.csv", index=False)
              
def cargar():
    df = pd.read_csv("/tmp/antropometria_indi.csv")
    scopes = {
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    }

    creds = ServiceAccountCredentials.from_json_keyfile_name("C:\Users\mota1\Documents\Dags\key_api\secrect_key.json", scopes=scopes)
    file = gspread.authorize(creds)
    workbook = file.open("ExampleAirflow")
    sheet = workbook.sheet1

    values = [df.columns.tolist()] + df.values.tolist()
    sheet.insert_rows(values, row=2)

    # Agregar una fila en blanco al final para separar los datos de futuras ejecuciones
    sheet.append_row([])

with DAG(
    'first',
    default_args=default_args,
    description='example',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example1'],
    
) as dag:
    extraer_task = PythonOperator(task_id="Extraer_datos", python_callable = extraer)
    procesar_task = PythonOperator(task_id="Calcular_indicadores", python_callable = trasnformar)
    cargar_task = PythonOperator(task_id="Cargar", python_callable = cargar)

    extraer_task >> procesar_task >> cargar_task
