import requests
import boto3
import os
import time

def start_scraping(patent_no, patent_url, bucket_name):
    # Create an S3 resource object for uploading
    s3 = boto3.resource('s3')
    
    # Use requests to fetch web content
    response = requests.get(patent_url)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Retrieve the content
        content = response.text
        
        # Create a filename and specify the /tmp directory
        file_name = f"/tmp/{patent_no}.txt"
        
        # Create a file object in the /tmp directory
        with open(file_name, 'w') as file:
            file.write(content)
        
        # Upload the file to S3
        s3.meta.client.upload_file(file_name, bucket_name, patent_no + ".txt")
        
        # Delete the temporary file after uploading
        os.remove(file_name)
    else:
        print(f"Failed to retrieve content from {patent_url}, status code: {response.status_code}")

# Initialize the S3
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Specify the S3 bucket name
    bucket_name = 'patent-bucket-raw'
    
    # Get the list of patent numbers and URLs from the event
    patent_nos = event.get('ids', [])
    patent_urls = event.get('result_links', [])
    
    # Iterate through the list of patent_nos and patent_urls
    for patent_no, patent_url in zip(patent_nos, patent_urls):
        # Call start_scraping function for each pair of patent_no and patent_url
        start_scraping(patent_no, patent_url, bucket_name)
        time.sleep(2)  # Pause for 2 seconds between requests