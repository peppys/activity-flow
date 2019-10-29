FROM puckel/docker-airflow:1.10.4

COPY requirements.txt .

RUN pip install --user -r requirements.txt
RUN pip install --user psycopg2-binary

ENV AIRFLOW_HOME=/usr/local/airflow

COPY airflow.cfg /usr/local/airflow/airflow.cfg
