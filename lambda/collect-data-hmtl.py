try:
    from selenium.webdriver import Chrome
    from selenium.webdriver.chrome.options import Options
    from bs4 import BeautifulSoup
    from datetime import timezone
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


def scrape_site(driver, url):
    try:
        driver.get(url)
        # Put the page source into a variable and create a BS object from it
        soup_file = driver.page_source
        soup = BeautifulSoup(soup_file)
        # Load and print the page content
        html = soup.find(id='content')
    except Exception as e:
        print("Issue extracting html. {}".format(e))
        
    return html
    
def get_options(site):
    try:
        s3_client = boto3.client('s3')
        config_key = 'scraper_config.csv'
        config_bucket = 'city-dataanalytics-raw'
        config_object = s3_client.get_object(Bucket=config_bucket, Key=config_key)
        config_data = config_object['Body'].read().decode('utf-8-sig')
        config = config_data.split("\n")
        config_rows = csv.DictReader(config)
        
        options = {}
        
        for row in config_rows:
            if row['site'] == site:
                options['site'] = row['site']
                options['url'] = row['url']
                options['element_id'] = row['element_id']
                options['s3_directory'] = row['s3_directory']
                
    except Exception as e:
        print("Error getting options. {}".format(e))
        
    return options

def lambda_handler(event, context):
    instance_ = WebDriver()
    driver = instance_.get()
    lambda_client = boto3.client('lambda')
    s3_client = boto3.client('s3')
    control_key = '{}_control.csv'.format(event['site'])
    bucket = 'city-dataanalytics-raw'
    destination_bucket = 'city-dataanalytics-staging'
    control_object = s3_client.get_object(Bucket=bucket, Key=control_key)
    
    data = control_object['Body'].read().decode('utf-8-sig')
    data_list = data.split("\n")
    data_dict = csv.DictReader(data_list)
    
    options = get_options(event['site'])
    directories = options['s3_directory'].replace(" ", "").split(',')
    print('Scraping..')
    
    for row in data_dict:
        if row['status'] == 'completed' or row['status'] == 'error':
            continue
        s3_path = []
        for directory in directories:
            s3_path.append(row[directory])
        url = row['url']
        html = scrape_site(driver, url)
        
        ## only write if page scraped successfully
        
        if html is None:
            row['status'] = 'error'
        else:
            row['status'] = 'completed'
            key = "{}/{}/{}/{}/{}.txt".format(options['site'], 'scrape', *s3_path, 'file')
            s3_client.put_object(Body=str(html), Bucket=destination_bucket, Key=key)
        
        row['site'] = options['site']
        row['date_completed'] = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        response = lambda_client.invoke(
            FunctionName = 'arn:aws:lambda:us-east-2:581630188109:function:update_control',
            InvocationType = 'RequestResponse',
            Payload = json.dumps(row)
        )
    
    driver.quit()
    return True