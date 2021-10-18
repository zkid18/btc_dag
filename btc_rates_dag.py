from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


import btc_rates


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    'coincap_parser',
    default_args=default_args,
    description='Parse Coinmarketcap to get rates',
    schedule_interval=timedelta(minutes=30)
) as dag:

    parse_coincap = PythonOperator(
        task_id='coincap_parser_task',
        python_callable=btc_rates.main,
        dag=dag
    )

    parse_coincap


