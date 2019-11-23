import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from github import OUTPUT_FILENAME as GITHUB_OUTPUT_FILENAME
from strava import OUTPUT_FILENAME as STRAVA_OUTPUT_FILENAME

GOOGLE_STORAGE_BUCKET = os.getenv('GOOGLE_STORAGE_BUCKET')


def compileactivity():
    hook = GoogleCloudStorageHook()

    github_response = hook.download(bucket=GOOGLE_STORAGE_BUCKET, object=GITHUB_OUTPUT_FILENAME)
    strava_response = hook.download(bucket=GOOGLE_STORAGE_BUCKET, object=STRAVA_OUTPUT_FILENAME)

    logging.info(f'Compiled github response ${github_response}')
    logging.info(f'Compiled strava response ${strava_response}')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 10, 26),
    'provide_context': True
}

dag = DAG(
    dag_id='activity',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

loaded_github_commits = ExternalTaskSensor(
    task_id='loaded_github_commits',
    external_dag_id='github',
    external_task_id='load_github_commits',
    allowed_states=['success'],
    execution_delta=timedelta(minutes=30),
    dag=dag,
)

loaded_strava_activity = ExternalTaskSensor(
    task_id='loaded_strava_activity',
    external_dag_id='strava',
    external_task_id='load_strava_activity',
    allowed_states=['success'],
    execution_delta=timedelta(minutes=30),
    dag=dag,
)

compile_activity = PythonOperator(
    task_id='compile_activity',
    python_callable=compileactivity,
    dag=dag,
)

[loaded_github_commits, loaded_strava_activity] >> compile_activity
