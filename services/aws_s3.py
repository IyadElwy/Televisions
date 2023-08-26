import boto3
from decouple import config
from io import BytesIO
import json

s3_resource = boto3.resource(
    's3',
    aws_access_key_id=config('AWS_ACCESS_KEY'),
    aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'),
    region_name=config('REGION'))

s3_client = s3_resource.meta.client


def convert_to_json_and_save_to_s3(data, bucket_name, file_name):
    if file_name.split('.')[-1] != 'json':
        raise Exception('File must be of type json')

    file_stream = BytesIO(json.dumps(data).encode())
    s3_client.upload_fileobj(
        file_stream, bucket_name, file_name)
