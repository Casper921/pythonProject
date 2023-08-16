import boto3
from boto3 import session
from botocore.exceptions import BotoCoreError
from utils import local_path, bucket_name, session


def upload_to_s3(file_name):
    local_file_path = f"{local_path}/{file_name}.csv"

    s3 = session.resource('s3')

    try:
        s3.meta.client.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=f"export/{file_name}.csv")
    except BotoCoreError as e:
        print(f'An error occurred: {e}')



