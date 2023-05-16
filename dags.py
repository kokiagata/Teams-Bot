from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from bot import runBots
import pandas as pd

default_args = {
    "email":['admin@admin.com'],
    "email_on_failure":True,
    "start_date": datetime(year=2023, month=5, day=5),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id= 'bot',
    default_args=default_args,
    schedule='*/15 12-23 * * 1-5'
)

with dag:
    bot = PythonOperator(
        task_id='runningBots',
        python_callable=runBots
    )
