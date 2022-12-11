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
s3 = boto3.client("s3")

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
    CONFIG_BUCKET_NAME = "my-bucket"

    # Key of the S3 object (i.e., the file name)
    CONFIG_OBJECT_KEY = "config.json"
    # Download the S3 object (i.e., the JSON config file)
    response = s3.get_object(Bucket=CONFIG_BUCKET_NAME, Key=CONFIG_OBJECT_KEY)
    
    # Convert the JSON config file to a dictionary
    config = response["Body"].read().decode("utf-8")
    
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
    data = element.get_text()
    
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

def update_csv_file(BUCKET_NAME, OBJECT_KEY, row):
    # Get the S3 object (i.e., the CSV file)
    response = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    
    # Convert the CSV file to a list of lists
    csv_file = list(csv.reader(response["Body"].read().decode("utf-8").splitlines()))
    
    # If the row is not None, update its status
    if row is not None:
        # Set the status of the row to "collected"
        row[1] = "collected"
        
        # Set the date_completed of the row to the current date and time
        row[2] = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        
        # Find the index of the row in the CSV file
        index = csv_file.index(row)
        
        # Update the row in the CSV file
        csv_file[index] = row
    else:
        # Set the status of the row to "error"
        csv_file[1] = "error"
        
    # Convert the CSV file back to a string
    csv_file_string = "\n".join([",".join(row) for row in csv_file])
    
    # Upload the updated CSV file to the S3 bucket
    s3.put_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY, Body=csv_file_string)

def lambda_handler(event, context):
    # Get the data_source and data_name from the event object
    data_source = event["data_source"]
    data_name = event["data_name"]
    
    # Use the get_options function to get the details of the specified data source and data name
    options = get_options(data_source, data_name)

    # Name of the S3 bucket
    BUCKET_NAME = "my-bucket"
    
    # Key of the S3 object (i.e., the file name)
    OBJECT_KEY = f"{event['data_name']}.csv"
    
    # Download the S3 object (i.e., the CSV file)
    response = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    
    # Open the CSV file
    csv_file = csv.reader(response["Body"].read().decode("utf-8").splitlines())

    # Check if the data source is a website or API
    if data_source == "website":
        # Iterate over the rows in the CSV file
        for row in csv_file:
            # Check if the second column in the row is empty
            if not row[1]:
                # Get the state and city values from the row
                page = row[0]
                state = row[2]
                city = row[3]

                # Get the current timestamp
                timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

                # Collect the data from the page
                page_data = collect_from_site(page, options["element_id"])

                # Generate the S3 object key (i.e., the file name) using the state, city, and timestamp
                object_key = f"{data_source}_{data_name}_{state}_{city}_{timestamp}.txt"
                
                # Write the collected data to the S3 bucket using the generated object key
                s3.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=page_data)

    elif data_source == "api":
        api_key = ''
        # Iterate over the rows in the CSV file
        for row in csv_file:
            # Check if the second column in the row is empty
            if not row[1]:
                # Get the state and city values from the row
                endpoint = row[0]
                state = row[2]
                city = row[3]

                # Get the current timestamp
                timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

                # Collect the data from the API using the collect_from_api function
                api_data = collect_from_api(endpoint, api_key)

                # Generate the S3 object key (i.e., the file name) using the state, city, and timestamp
                object_key = f"{data_source}_{data_name}_{state}_{city}_{timestamp}.txt"
                
                # Write the collected data to the S3 bucket using the generated object key
                s3.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=api_data)
