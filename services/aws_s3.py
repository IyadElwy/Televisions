import boto3
from decouple import config
from io import BytesIO
import json


s3_client = boto3.client(
    's3',
    aws_access_key_id=config('AWS_ACCESS_KEY'),
    aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'),
    region_name=config('REGION'))


def save_titles_url_to_s3(data_as_list, file_name):
    if file_name.split('.')[-1] != 'json':
        raise Exception('File must be of type json')

    file_stream = BytesIO(json.dumps(data_as_list).encode())
    s3_client.upload_fileobj(
        file_stream, 'televisions-raw-titles-urls', file_name)
