from google.cloud import storage


def download_gcs_file(bucket: str, filename: str, project_id: str):
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(filename)
    blob.download_as_string()
    blob.upload_from_string(data=data, content_type=content_type)


def upload_gcs_file(data, filename: str, content_type: str, project_id: str, bucket: str):
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(filename)
    blob.upload_from_string(data=data, content_type=content_type)
