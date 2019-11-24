import tempfile

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.http_operator import SimpleHttpOperator


class HttpToGcsOperator(SimpleHttpOperator):
    def __init__(self,
                 bucket: str,
                 filename: str,
                 google_cloud_storage_conn_id: str = 'google_cloud_default',
                 mime_type: str = 'application/json',
                 *args,
                 **kwargs):
        super(HttpToGcsOperator, self).__init__(xcom_push=True, *args, **kwargs)
        self.bucket = bucket
        self.filename = filename
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.mime_type = mime_type

    def execute(self, context):
        response = super().execute(context)

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)

        with tempfile.NamedTemporaryFile(prefix="gcs-local") as file:
            file.write(response.encode('utf-8'))
            file.flush()

            hook.upload(
                bucket=self.bucket,
                filename=file.name,
                object=self.filename,
                mime_type=self.mime_type
            )
