from datetime import timedelta, datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 10, 26)
}

dag = DAG(
    dag_id='print_stuff',
    default_args=default_args,
    schedule_interval=timedelta(seconds=10)
)

t1 = PythonOperator(
    task_id='print_hello_world',
    python_callable=lambda: logging.info('Hello world'),
    dag=dag,
)

t2 = PythonOperator(
    task_id='print_goodbye_world',
    python_callable=lambda: logging.info('Goodbye world'),
    dag=dag,
)

t1 >> t2
