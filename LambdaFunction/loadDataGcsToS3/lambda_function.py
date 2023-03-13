import json
import boto3
from time import time
import datetime
import urllib.request
import os
import requests

webhook_url = "https://hooks.slack.com/services/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"

t0 = time()
gc_bucket="dont8_bucket"
s3_bucket="dont8"


def lambda_handler(event, context):
    
    try:
        count_file = 0
        s3_client = boto3.client('s3')
        alert = ''
        google_access_key_id, google_access_key_secret = get_key()
        gcs_client= connect_gcs(google_access_key_id, google_access_key_secret)
        paginator = gcs_client.get_paginator('list_objects_v2')
        gcs_data = paginator.paginate(Bucket=gc_bucket) # gc_bucket
        for page in gcs_data:
            print("Page: ", page)
            for i in page['Contents']:
                print(f"Copying gcs file: [{i['Key']}] to AWS S3 bucket: [{s3_bucket}]")
                file_to_transfer = gcs_client.get_object(Bucket=gc_bucket, Key=i['Key'])
                s3_client.upload_fileobj(file_to_transfer['Body'], s3_bucket, Key=i['Key'])
                count_file = count_file + 1
                time_run = time()-t0
                time_now = datetime.datetime.now()
                date_time = time_now.strftime("%H:%M:%S")
                text = f"Upload {i['Key']} file DONE in " + str('{0:.2f}'.format(time_run)) + f" sec(s) on {date_time}\n\n"
                alert = alert + text
      
        log_stream_url = get_log()
        count_file = str(count_file)
        notification = f'Ingest data {time_now.strftime("%Y-%m-%d")} \n' + alert + f'There are {count_file} to be ingest \n' + f'Your function cloudwatch URL: {log_stream_url} \n'
        send_alert_slack(notification)
    except Exception as e:
        time_now = datetime.datetime.now()
        date_time = time_now.strftime("%H:%M:%S")
        log_stream_url = get_log()
        text = f"Your lambda function is faulty on {date_time}\nError Name: {e}\nYour function cloudwatch URL: {log_stream_url}"
        send_alert_slack(text)

def get_key():
    ssms_client = boto3.client('ssm')
    google_access_key_id = ssms_client.get_parameter(Name='google_access_key_id', WithDecryption=True)['Parameter']['Value']
    google_access_key_secret = ssms_client.get_parameter(Name='google_access_key_secret', WithDecryption=True)['Parameter']['Value']
    return google_access_key_id, google_access_key_secret

def connect_gcs(google_access_key_id, google_access_key_secret):
    gcs_client = boto3.client('s3',
                    region_name="auto",
                    endpoint_url="https://storage.googleapis.com",
                    aws_access_key_id=google_access_key_id,
                    aws_secret_access_key=google_access_key_secret
                    )
    return gcs_client

def get_log():
    log_group_name = os.environ['AWS_LAMBDA_LOG_GROUP_NAME']
    region = os.environ['AWS_REGION']
    log_group_name_encode = log_group_name.replace('/', '$252F')
    log_stream_name_encode = log_group_name.replace('$', '$2524').replace('[', '$255B').replace(']', '$255D').replace('/', '$252F')
    log_stream_url = f"https://console.aws.amazon.com/cloudwatch/home?region={region}#logsV2:log-groups/log-group/{log_group_name_encode}"
    return log_stream_url

   
def send_alert_slack(text):
    slack_data = {
            'channel': 'cloudwatch-alerts',
            'text': text
        }
    requests.post(url=webhook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
    return("success")