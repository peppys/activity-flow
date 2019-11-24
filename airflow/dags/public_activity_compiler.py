import json
import os
import tempfile
from datetime import datetime
import dateutil

from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator

from github_commits_loader import OUTPUT_FILENAME as GITHUB_OUTPUT_FILENAME
from strava_activity_loader import OUTPUT_FILENAME as STRAVA_OUTPUT_FILENAME

GOOGLE_STORAGE_BUCKET = os.getenv('GOOGLE_STORAGE_BUCKET')
OUTPUT_FILENAME = 'index.html'


def compileactivity():
    hook = GoogleCloudStorageHook()

    github_response = hook.download(bucket=GOOGLE_STORAGE_BUCKET, object=GITHUB_OUTPUT_FILENAME)
    strava_response = hook.download(bucket=GOOGLE_STORAGE_BUCKET, object=STRAVA_OUTPUT_FILENAME)

    github_response_json = json.loads(github_response.decode("utf-8"))
    strava_response_json = json.loads(strava_response.decode("utf-8"))

    cleaned_github_commits = list(map(
        lambda item: {
            'created_at': dateutil.parser.parse(item['commit']['committer']['date']).isoformat(),
            'username': item['committer']['login'],
            'url': item['html_url'],
            'sha': item['sha'],
            'message': item['commit']['message'],
            'repo': item['repository']['full_name']}, github_response_json['items']))

    cleaned_github_commits.sort(
        key=lambda x: dateutil.parser.parse(x['created_at']), reverse=True)

    cleaned_strava_activity = list(map(
        lambda ride: {'created_at': dateutil.parser.parse(ride['start_date']).isoformat(),
                      'name': ride['name'],
                      'distance_miles': round(ride['distance'] / 1609.34, 2),
                      'type': ride['type'],
                      'elapsed_time_seconds': ride['elapsed_time']}, strava_response_json))

    cleaned_strava_activity.sort(
        key=lambda x: dateutil.parser.parse(x['created_at']), reverse=True)

    public_activity = {
        'github': cleaned_github_commits[0:4],
        'strava': cleaned_strava_activity[0:4],
    }

    hook = GoogleCloudStorageHook()

    with tempfile.NamedTemporaryFile(prefix="gcs-local") as file:
        file.write(json.dumps(public_activity).encode('utf-8'))
        file.flush()

        hook.upload(
            bucket=GOOGLE_STORAGE_BUCKET,
            filename=file.name,
            object=OUTPUT_FILENAME,
            mime_type='application/json'
        )
        hook.insert_object_acl(
            bucket=GOOGLE_STORAGE_BUCKET,
            object_name=OUTPUT_FILENAME,
            entity='allUsers',
            role='READER',
        )


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 10, 26),
}

dag = DAG(
    dag_id='public_activity_compiler',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

compile_activity = PythonOperator(
    task_id='compile_public_activity',
    python_callable=compileactivity,
    dag=dag,
)
