from datetime import timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

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
    df = pd.read_csv("/tmp/antropometria.csv")
    logging(df.head)

def cargar():
    logging.info("saving")

with DAG(
    'first',
    default_args=default_args,
    description='example',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example1'],
    
) as dag:
    extraer_task = PythonOperator(task_id="extraer", python_callable = extraer)
    trasnformar_task = PythonOperator(task_id="trasnformar", python_callable = trasnformar)
    cargar_task = PythonOperator(task_id="cargar", python_callable = cargar)

    extraer_task >> trasnformar_task >> cargar_task
