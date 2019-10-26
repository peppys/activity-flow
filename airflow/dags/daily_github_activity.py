import json
from datetime import timedelta, datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 10, 26)
}

dag = DAG(
    dag_id='daily_github_activity',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

sensor = HttpSensor(
    task_id='github_commits_sensor',
    http_conn_id='',
    headers={'Accept': 'application/vnd.github.cloak-preview'},
    endpoint='https://api.github.com/search/commits?q=committer:peppys+committer-date:{{ macros.ds_add(ds, -1) }}..{{ ds }}&sort=committer-date',
    response_check=lambda response: response.json()['total_count'] > 0,
    dag=dag
)

t1 = PythonOperator(
    task_id='print_found_commits',
    python_callable=lambda: logging.info('We found commits'),
    dag=dag,
)

sensor >> t1
