from google.cloud import storage


def upload_gcs_file(data, filename: str, content_type: str, project_id: str, bucket: str):
    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(filename)
    blob.upload_from_string(data=data, content_type=content_type)
