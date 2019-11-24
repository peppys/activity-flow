from datetime import timedelta, datetime
import os

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.http_sensor import HttpSensor

from operators.http_to_gcs_operator import HttpToGcsOperator

GITHUB_USERNAME = os.getenv('GITHUB_USERNAME')
GOOGLE_STORAGE_BUCKET = os.getenv('GOOGLE_STORAGE_BUCKET')
OUTPUT_FILENAME = 'github-commits.json'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 10, 26),
    'provide_context': True
}

dag = DAG(
    dag_id='github_commits_loader',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
)

check_commits = HttpSensor(
    task_id='pull_commits',
    http_conn_id='',
    headers={'Accept': 'application/vnd.github.cloak-preview'},
    method='GET',
    endpoint=f'https://api.github.com/search/commits?q=committer:{GITHUB_USERNAME}&sort=committer-date',
    response_check=lambda response: response.json()['total_count'] > 0,
    dag=dag,
)

load_github_commits = HttpToGcsOperator(
    task_id='load_github_commits',
    http_conn_id='',
    headers={'Accept': 'application/vnd.github.cloak-preview'},
    method='GET',
    endpoint=f'https://api.github.com/search/commits?q=committer:{GITHUB_USERNAME}&sort=committer-date',
    bucket=GOOGLE_STORAGE_BUCKET,
    filename=OUTPUT_FILENAME,
    dag=dag,
)

trigger_activity_compiler = TriggerDagRunOperator(
    task_id='trigger_activity_compiler',
    trigger_dag_id='public_activity_compiler',
    dag=dag,
)

check_commits >> load_github_commits >> trigger_activity_compiler
