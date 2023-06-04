from datetime import timedelta

from airflow import DAG

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

def scrape():
    logging.info("scrapping")

def process():
    logging.info("processing")

def save():
    logging.info("saving")

with DAG(
    'first',
    default_args=default_args,
    description='example',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example1'],
    
) as dag:
    scrape_task = PythonOperator(task_id="scrape", python_callable = scrape)
    process_task = PythonOperator(task_id="process", python_callable = process)
    save_task = PythonOperator(task_id="save", python_callable = save)

    scrape_task >> process_task >> save_task