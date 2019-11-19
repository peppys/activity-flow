# from datetime import datetime, timedelta
#
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.sensors.external_task_sensor import ExternalTaskSensor
#
#
# # def compileactivity():
#
#
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2019, 10, 26),
#     'provide_context': True
# }
#
# dag = DAG(
#     dag_id='activity',
#     default_args=default_args,
#     schedule_interval=timedelta(days=1)
# )
#
# loaded_github_commits = ExternalTaskSensor(
#     task_id='loaded_github_commits',
#     external_dag_id='github',
#     external_task_id='load_github_commits',
#     allowed_states=['success'],
#     execution_delta=timedelta(minutes=30),
# )
#
# loaded_strava_activity = ExternalTaskSensor(
#     task_id='loaded_strava_activity',
#     external_dag_id='strava',
#     external_task_id='load_strava_activity',
#     allowed_states=['success'],
#     execution_delta=timedelta(minutes=30),
# )
#
# compile_activity = PythonOperator(
#     task_id='fetch_tweets',
#     python_callable=compileactivity,
#     dag=dag,
# )
#
# [loaded_github_commits, loaded_strava_activity] >> compile_activity
