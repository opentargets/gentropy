"""Common functions for Google cloud storage (GCS) operations."""


def access_gcp_secret(secret_id: str, project_id: str) -> str:
    """Access GCP secret manager to get the secret value.

    Args:
        secret_id (str): ID of the secret
        project_id (str): ID of the GCP project

    Returns:
        str: secret value
    """
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def copy_to_gcs(source_path: str, destination_blob: str) -> None:
    """Copy a file to a Google Cloud Storage bucket.

    Args:
        source_path (str): Path to the local file to copy
        destination_blob (str): GS path to the destination blob in the GCS bucket

    Raises:
        ValueError: If the path is a directory
    """
    import os
    from urllib.parse import urlparse

    from google.cloud import storage

    if os.path.isdir(source_path):
        raise ValueError("Path should be a file, not a directory.")
    client = storage.Client()
    bucket = client.bucket(bucket_name=urlparse(destination_blob).hostname)
    blob = bucket.blob(blob_name=urlparse(destination_blob).path.lstrip("/"))
    blob.upload_from_filename(source_path)
