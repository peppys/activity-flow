from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.http_operator import SimpleHttpOperator


class HttpToGcsOperator(SimpleHttpOperator):
    def __init__(self, project_id: str, bucket: str,
                 filename: str, google_cloud_storage_conn_id: str = 'google_cloud_default', *args,
                 **kwargs):
        super(HttpToGcsOperator, self).__init__(xcom_push=True, *args, **kwargs)
        self.project_id = project_id
        self.bucket = bucket
        self.filename = filename
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id

    def execute(self, context):
        response = super().execute(context)

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)

        hook.upload(
            bucket=self.bucket,
            filename=self.filename,
            object=response
        )
