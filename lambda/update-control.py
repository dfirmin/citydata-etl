import json
import pandas as pd
import boto3
import io
import os

def lambda_handler(event, context):
    # TODO implement
    s3_client = boto3.client('s3')
    control_key = '{}_control.csv'.format(event['site'])
    config_key = 'scraper_config.csv'
    bucket = 'city-dataanalytics-raw'
    control_response = s3_client.get_object(Bucket=bucket, Key=control_key)
    config_response = s3_client.get_object(Bucket=bucket, Key=config_key)
    control_status = control_response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    config_status = control_response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if control_status == 200 and config_status == 200:
        print(f"Successful S3 get_object response. Status - {control_status}{config_status}")
        control_df = pd.read_csv(control_response.get("Body"))
        config_df = pd.read_csv(config_response.get("Body"))
        config_row = config_df[(config_df.site == event['site'])]
        filters = config_row['s3_directory'].iloc[0].replace(" ", "").split(",")
        update_df = control_df
        for filter in filters:
            update_df = update_df[(update_df[filter] == event[filter])]
            
        # # df1.loc[df1['stream'] == 2, ['feat','another_feat']] = 'aaaa'
        control_df.loc[control_df['id'] == update_df['id'].iloc[0], ['status']] = event['status']
        control_df.loc[control_df['id'] == update_df['id'].iloc[0], ['date_completed']] = event['date_completed']
        
        with io.StringIO() as csv_buffer:
            control_df.to_csv(csv_buffer, index=False)
            response = s3_client.put_object(
                Bucket=bucket, Key=control_key, Body=csv_buffer.getvalue()
                )
        return {
        'statusCode': 200,
        'body': json.dumps('Control File Updated!')
        }
    else:
        print(f"Unsuccessful S3 get_object response. Status - {control_status}{config_status}")
        return {
        'statusCode': 400,
        'body': json.dumps('Control File Failed to Update!')
        }
