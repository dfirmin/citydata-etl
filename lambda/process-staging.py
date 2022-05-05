import sys
import boto3
import datetime
import awswrangler as wr

client = boto3.client('s3')

# Set source and target bucket for data to be read/written to
sourceBucketName = 'city-dataanalytics-staging'
targetBucketName = 'city-dataanalytics-raw'


# Get current year/month/day/hour from dt object
dt = datetime.datetime.today()
year = dt.year
month = dt.month
day = dt.day
hour = dt.hour

# object list will contain all current files in staging
objectList = wr.s3.list_objects('s3://'+sourceBucketName)

# object contain the "path" including the file's name (ex:s3://testbucket/public/testtable/LOAD00000001.parquet)
for obj in objectList:
    #strip the bucket name to obtain the objects key
    key = obj.replace('s3://{}/'.format( sourceBucketName),"")
    copySource = {
        'Bucket': sourceBucketName,
        'Key': key
    }

# Get the source, schema and table from the file path and create target path, "load" is also included to determine if its a intial or cdc file. Manual drop-ins default to initial
    path = key.split('/')
    source = path[0]
    if source == 'manual':
        name = path[1]
        table = path[1].replace('.csv', '')
        newFilePath = "{}/{}/{}/{}/{}/{}/{}/{}/{}".format("manual", "misc", table, "initial", year, month, day, hour, name)
    else:
        schema = path[1]
        table = path[2] 
        load = path[3]
        name = path[4]
        newFilePath = "{}/{}/{}/{}/{}/{}/{}/{}/{}".format(source, schema, table, load, year, month, day, hour, name)
# Copy the object and dump it into the target bucket with the new key
    client.copy_object(
        Bucket = targetBucketName,
        CopySource = copySource,
        Key = newFilePath
        )
# After the files have been moved, delete the original from the source bucket.
    client.delete_object(
        Bucket = sourceBucketName,
        Key = key
    )