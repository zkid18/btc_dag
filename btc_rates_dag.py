from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


import btc_rates


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
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

    t1 = PythonOperator(
        task_id='coincap_parser_task',
        python_callable=btc_rates.main,
        dag=dag
    )

    t1


