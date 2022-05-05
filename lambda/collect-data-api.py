import json
import urllib3
import os
import boto3
import csv
import datetime

def lambda_handler(event, context):
    lambda_client = boto3.client('lambda')
    s3_client = boto3.client('s3')
    control_key = '{}_control.csv'.format(event['api'])
    bucket = 'city-dataanalytics-raw'
    destination_bucket = 'city-dataanalytics-staging'
    control_object = s3_client.get_object(Bucket=bucket, Key=control_key)
    
    data = control_object['Body'].read().decode('utf-8-sig')
    data_list = data.split("\n")
    data_dict = csv.DictReader(data_list)
    
    options = get_options(event['api'])
    directories = options['s3_directory'].replace(" ", "").split(',')
    parameters = options['parameters'].replace(" ", "").split(',')
    
    http = urllib3.PoolManager()
    # TODO implement
    try:
        for row in data_dict:
            if row['status'] == 'completed' or row['status'] == 'error':
                return
            
            api_parameters = map(lambda x: row[x], parameters)
            endpoint_params ='?{}=1&{}=2'.format(options['endpoint'], parameters*)
            response = http.request(
                'GET', 
                '{}?{}=mi&{}=48316'.format(options['endpoint'], parameters*),
                headers={
                    "x-api-key": apikey
                }
            )
            print(response.data)
            ## check response
            # if response.status == 200
            
            s3_path = map(lambda x: row[x], directories)
        return 
    except Exception as e:
        print(e)
        return 404
def get_options(api):
    try:
        s3_client = boto3.client('s3')
        config_key = 'api_config.csv'
        config_bucket = 'city-dataanalytics-raw'
        config_object = s3_client.get_object(Bucket=config_bucket, Key=config_key)
        config_data = config_object['Body'].read().decode('utf-8-sig')
        config = config_data.split("\n")
        config_rows = csv.DictReader(config)
        
        options = {}
        
        for row in config_rows:
            if row['api'] == api:
                options['api'] = row['api']
                options['endpoint'] = row['endpoint']
                options['parameters'] = row['parameters']
                options['s3_directory'] = row['s3_directory']
                
    except Exception as e:
        print("Error getting options. {}".format(e))
        
    return options