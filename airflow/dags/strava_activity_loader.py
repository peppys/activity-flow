from datetime import timedelta, datetime
import os

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from operators.oauth_http_to_gcs_operator import OAuthHttpToGcsOperator
from sensors.oauth_http_sensor import OAuthHttpSensor

STRAVA_CLIENT_ID = os.getenv('STRAVA_CLIENT_ID')
STRAVA_CLIENT_SECRET = os.getenv('STRAVA_CLIENT_SECRET')
STRAVA_REFRESH_TOKEN = os.getenv('STRAVA_REFRESH_TOKEN')

GOOGLE_STORAGE_BUCKET = os.getenv('GOOGLE_STORAGE_BUCKET')
OUTPUT_FILENAME = 'strava-activity.json'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 10, 26),
    'provide_context': True
}

dag = DAG(
    dag_id='strava_activity_loader',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
)

check_strava_activity = OAuthHttpSensor(
    task_id='pull_strava_activity',
    http_conn_id='',
    method='GET',
    endpoint='https://www.strava.com/api/v3/athlete/activities',
    oauth_endpoint='https://www.strava.com/oauth/token',
    oauth_body={'client_id': STRAVA_CLIENT_ID, 'client_secret': STRAVA_CLIENT_SECRET,
                'grant_type': 'refresh_token', 'refresh_token': STRAVA_REFRESH_TOKEN},
    oauth_response=lambda response: response.json()['access_token'],
    response_check=lambda response: len(response.json()) > 0,
    dag=dag,
)

load_strava_activity = OAuthHttpToGcsOperator(
    task_id='load_strava_activity',
    http_conn_id='',
    method='GET',
    endpoint='https://www.strava.com/api/v3/athlete/activities',
    oauth_endpoint='https://www.strava.com/oauth/token',
    oauth_body={'client_id': STRAVA_CLIENT_ID, 'client_secret': STRAVA_CLIENT_SECRET,
                'grant_type': 'refresh_token', 'refresh_token': STRAVA_REFRESH_TOKEN},
    oauth_response=lambda response: response.json()['access_token'],
    bucket=GOOGLE_STORAGE_BUCKET,
    filename=OUTPUT_FILENAME,
    dag=dag,
)

trigger_activity_compiler = TriggerDagRunOperator(
    task_id='trigger_activity_compiler',
    trigger_dag_id='public_activity_compiler',
    dag=dag,
)

check_strava_activity >> load_strava_activity >> trigger_activity_compiler
