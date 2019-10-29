from datetime import timedelta, datetime
import os
import json

from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator

from google.cloud import storage

GITHUB_USERNAME = os.getenv('GITHUB_USERNAME')
GOOGLE_PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID')
GOOGLE_STORAGE_BUCKET = os.getenv('GOOGLE_STORAGE_BUCKET')
OUTPUT_FILENAME = 'github.json'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 10, 26),
    'provide_context': True
}

dag = DAG(
    dag_id='github_activity',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

pull_commits = SimpleHttpOperator(
    task_id='pull_commits',
    http_conn_id='',
    headers={'Accept': 'application/vnd.github.cloak-preview'},
    method='GET',
    endpoint=f'https://api.github.com/search/commits?q=committer:{GITHUB_USERNAME}&sort=committer-date',
    response_check=lambda response: response.json()['total_count'] > 0,
    xcom_push=True,
    dag=dag,
)


def transform(**kwargs):
    commits_response = kwargs['ti'].xcom_pull(task_ids=pull_commits.task_id)
    commits_response_json = json.loads(commits_response)

    commits = list(map(
        lambda item: {'created_at': item['commit']['committer']['date'],
                      'username': item['committer']['login'],
                      'url': item['html_url'],
                      'sha': item['sha'],
                      'message': item['commit']['message'],
                      'repo': item['repository']['full_name']}, commits_response_json['items']))

    return commits


transform_commits = PythonOperator(
    task_id='transform_commits',
    python_callable=transform,
    do_xcom_push=True,
    dag=dag,
)


def load(**kwargs):
    commits = kwargs['ti'].xcom_pull(task_ids=transform_commits.task_id)

    client = storage.Client(project=GOOGLE_PROJECT_ID)
    bucket = client.get_bucket(GOOGLE_STORAGE_BUCKET)
    blob = bucket.blob(OUTPUT_FILENAME)
    blob.upload_from_string(data=json.dumps(commits), content_type='application/json')


load_commits = PythonOperator(
    task_id='load_commits',
    python_callable=load,
    dag=dag,
)

pull_commits >> transform_commits >> load_commits
