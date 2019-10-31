from datetime import timedelta, datetime
import os
import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from operators.oauth_http_operator import OAuthHttpOperator

from lib.google_storage import upload_gcs_file

STRAVA_CLIENT_ID = os.getenv('STRAVA_CLIENT_ID')
STRAVA_CLIENT_SECRET = os.getenv('STRAVA_CLIENT_SECRET')
STRAVA_REFRESH_TOKEN = os.getenv('STRAVA_REFRESH_TOKEN')

GOOGLE_PROJECT_ID = os.getenv('GOOGLE_PROJECT_ID')
GOOGLE_STORAGE_BUCKET = os.getenv('GOOGLE_STORAGE_BUCKET')
OUTPUT_FILENAME = 'strava.json'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 10, 26),
    'provide_context': True
}

dag = DAG(
    dag_id='strava_activity',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

pull_strava_activity = OAuthHttpOperator(
    task_id='pull_strava_activity',
    http_conn_id='',
    method='GET',
    endpoint='https://www.strava.com/api/v3/athlete/activities',
    oauth_endpoint='https://www.strava.com/oauth/token',
    oauth_body={'client_id': STRAVA_CLIENT_ID, 'client_secret': STRAVA_CLIENT_SECRET,
                'grant_type': 'refresh_token', 'refresh_token': STRAVA_REFRESH_TOKEN},
    oauth_response=lambda response: response.json()['access_token'],
    xcom_push=True,
    dag=dag,
)


def transform(**kwargs):
    strava_activity_response = kwargs['ti'].xcom_pull(task_ids=pull_strava_activity.task_id)
    strava_activity_response_json = json.loads(strava_activity_response)

    strava_activity = list(map(
        lambda ride: {'created_at': ride['start_date'],
                      'name': ride['name'],
                      'distance_miles': ride['distance'] / 1609.34,
                      'type': ride['type'],
                      'elapsed_time': ride['elapsed_time']}, strava_activity_response_json))

    return strava_activity


transform_strava_activity = PythonOperator(
    task_id='transform_strava_activity',
    python_callable=transform,
    do_xcom_push=True,
    dag=dag,
)


def load(**kwargs):
    commits = kwargs['ti'].xcom_pull(task_ids=transform_strava_activity.task_id)

    upload_gcs_file(
        data=json.dumps(commits),
        content_type='application/json',
        filename=OUTPUT_FILENAME,
        project_id=GOOGLE_PROJECT_ID,
        bucket=GOOGLE_STORAGE_BUCKET,
    )


load_strava_activity = PythonOperator(
    task_id='load_strava_activity',
    python_callable=load,
    do_xcom_push=True,
    dag=dag,
)

pull_strava_activity >> transform_strava_activity >> load_strava_activity
