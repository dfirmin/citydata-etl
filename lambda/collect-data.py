try:
    from selenium.webdriver import Chrome
    from selenium.webdriver.chrome.options import Options
    from bs4 import BeautifulSoup
    from datetime import timezone
    import requests
    import io
    import json
    import os
    import shutil
    import uuid
    import boto3
    import csv
    import datetime

    print("All Modules are ok ...")

except Exception as e:

    print("Error in Imports. {}".format(e))

# Amazon S3 client
s3_client = boto3.client("s3")

class WebDriver(object):

    def __init__(self):
        self.options = Options()

        self.options.binary_location = '/opt/headless-chromium'
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--start-maximized')
        self.options.add_argument('--start-fullscreen')
        self.options.add_argument('--single-process')
        self.options.add_argument('--disable-dev-shm-usage')

    def get(self):
        driver = Chrome('/opt/chromedriver', options=self.options)
        return driver

def get_options(data_source, source_name):
    """
    Read the JSON config file from an Amazon S3 bucket and return the details of the specified data source and source name.
    
    data_source: The data source to collect the data from (e.g., "website" or "api")
    source_name: The name of the website or API to collect the data from
    
    Returns: The details of the specified data source and source name as a dictionary
    """
    # Name of the S3 bucket
    CONFIG_BUCKET_NAME = "city-dataanalytics-raw"

    # Key of the S3 object (i.e., the file name)
    CONFIG_OBJECT_KEY = "config.json"
    # Download the S3 object (i.e., the JSON config file)
    response = s3_client.get_object(Bucket=CONFIG_BUCKET_NAME, Key=CONFIG_OBJECT_KEY)
    
    # Convert the JSON config file to a dictionary
    config_str = response["Body"].read().decode("utf-8")
    config = json.loads(config_str)

    # Return the details of the specified data source and source name
    return config[data_source][source_name]

def collect_from_site(site_url, element_id):
    """
    Collect data from a website using the BeautifulSoup library.
    
    site_url: The URL of the website to scrape
    element_id: The ID of the HTML element to scrape the data from
    
    Returns: The scraped data as a string
    """
    # Make an HTTP GET request to the specified site_url
    instance_ = WebDriver()
    driver = instance_.get()
    driver.get(site_url)
    # Put the page source into a variable and create a BS object from it
    soup_file = driver.page_source

    # Parse the HTML content of the website using BeautifulSoup
    soup = BeautifulSoup(soup_file)
    
    # Find the HTML element with the specified element_id
    element = soup.find(id=element_id)
    
    # Extract the text from the HTML element
    data = element
    
    return data


def collect_from_api(endpoint_url, endpoint_params):
    """
    Collect data from an API endpoint using the requests library.
    
    endpoint_url: The URL of the API endpoint
    endpoint_params: The parameters to pass to the API endpoint as a dictionary
    
    Returns: The data returned by the API endpoint as a dictionary
    """
    # Make an HTTP GET request to the specified endpoint_url
    r = requests.get(endpoint_url, params=endpoint_params)
    
    # Convert the response to a dictionary
    data = r.json()
    
    return data

def update_csv_file(bucket_name, object_key, updated_row, csv_file):
    s3_client.download_file(bucket_name, object_key, '/tmp/original_file.csv')
    # Set the date_completed of the row to the current date and time
    updated_row['date_completed'] = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    
    # Open the original file in read mode
    with open('/tmp/original_file.csv', 'r') as original_file:
        # Open a new file in write mode
        with open('/tmp/new_file.csv', 'w') as new_file:
            # Create a new CSV writer
            writer = csv.DictWriter(new_file, fieldnames=csv_file.fieldnames)
            # Write the header row
            writer.writeheader()
    
            # Iterate over the rows in the original CSV file
            for row in csv.DictReader(original_file):
                # If the current row is the updated row, write the updated row to the new file
                if row['id'] == updated_row['id']:
                    writer.writerow(updated_row)
                else:
                    # Otherwise, write the original row to the new file
                    writer.writerow(row)
    
    # Upload the new file to S3
    s3_client.upload_file('/tmp/new_file.csv', bucket_name, object_key)




def lambda_handler(event, context):
    # Get the data_source and data_name from the event object
    data_source = event["data_source"]
    source_name = event["source_name"]
    
    # Use the get_options function to get the details of the specified data source and data name
    options = get_options(data_source, source_name)

    # Name of the S3 bucket
    CONTROL_BUCKET_NAME = "city-dataanalytics-raw"
    TARGET_BUCKET_NAME = "city-dataanalytics-staging"
    
    # Key of the S3 object (i.e., the file name)
    OBJECT_KEY = f"{event['source_name']}_control.csv"
    
    # Download the S3 object (i.e., the CSV control file)
    response = s3_client.get_object(Bucket=CONTROL_BUCKET_NAME, Key=OBJECT_KEY)
    
    # Open the CSV file
    csv_file = csv.DictReader(response["Body"].read().decode("utf-8").splitlines())
    
    # Check if the data source is a website or API
    if data_source == "website":
        # Iterate over the rows in the CSV file
        for row in csv_file:
            # Check if the second column in the row is empty
            if not row['status'] and row['id']:
                # Get the site, state and city values from the row
                page = row['url']
                state = row['state']
                city = row['city']

                # Get the current timestamp
                timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

                # Collect the data from the page
                page_data = collect_from_site(page, options["element_id"])
                if page_data is None:
                    row['status'] = 'error'
                    update_csv_file(CONTROL_BUCKET_NAME, OBJECT_KEY, row, csv_file)
                else:
                    row['status'] = 'completed'
                    # Generate the S3 object key (i.e., the file name) using the state, city, and timestamp
                    object_key = f"{data_source}/{source_name}/{state}/{city}/{timestamp}.txt"
                    
                    # Write the collected data to the S3 bucket using the generated object key
                    s3_client.put_object(Bucket=TARGET_BUCKET_NAME, Key=object_key, Body=str(page_data))
                    update_csv_file(CONTROL_BUCKET_NAME, OBJECT_KEY, row, csv_file)


    elif data_source == "api":
        # Iterate over the rows in the CSV file
        for row in csv_file:
            # Check if the second column in the row is empty
            if not row[3]:
                # Get the state and city values from the row
                endpoint = row['endpoint']
                state = row['state']
                city = row['city']

                # Get the current timestamp
                timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

                # Collect the data from the API using the collect_from_api function
                api_data = collect_from_api(endpoint, options["api_key"])

                # Generate the S3 object key (i.e., the file name) using the state, city, and timestamp
                object_key = f"{data_source}_{data_name}_{state}_{city}_{timestamp}.txt"
                
                # Write the collected data to the S3 bucket using the generated object key
                s3_client.put_object(Bucket=TARGET_BUCKET_NAME, Key=object_key, Body=api_data)
