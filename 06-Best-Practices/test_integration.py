import boto3
import os
import pytest

# Localstack endpoint URL
S3_ENDPOINT_URL = 'http://localhost:4566'

# Initialize S3 client
s3_client = boto3.client('s3', endpoint_url=S3_ENDPOINT_URL)

def upload_to_s3(bucket_name, key, local_file_path):
    s3_client.upload_file(local_file_path, bucket_name, key)
    print(f"Uploaded {local_file_path} to s3://{bucket_name}/{key}")

def test_upload_sample_text_file():
    bucket_name = 'nyc-duration'
    key = 'sample-file.txt'
    local_file_path = '/workspaces/mlops-zoomcamp/06-Best-Practices/sample-file.txt'

    # Upload sample text file to Localstack S3 bucket
    upload_to_s3(bucket_name, key, local_file_path)

    # Verify the file is uploaded by listing objects in the bucket
    response = s3_client.list_objects(Bucket=bucket_name)
    assert 'Contents' in response
    assert any(obj['Key'] == key for obj in response['Contents']), f"Uploaded file {key} not found in bucket {bucket_name}"

if __name__ == '__main__':
    pytest.main()
