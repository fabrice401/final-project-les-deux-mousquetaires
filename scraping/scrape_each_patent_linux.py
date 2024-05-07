import requests
import boto3
import os
import time
import pandas as pd

# Initialize the S3
s3 = boto3.resource('s3',
                    aws_access_key_id='ASIA4HHT4VBDAYLGL342',#secrets.AWS_ACCESS_KEY,
                    aws_secret_access_key='MY0uzXM+ooVxSZjdDtfVLpCJni3wMLWJcQjEG4mU',#secrets.AWS_SECRET_KEY,
                    aws_session_token='IQoJb3JpZ2luX2VjEIT//////////wEaCXVzLXdlc3QtMiJGMEQCIClIafBJZsVKXD8rVvR/BxK3NFvz1OahIdFTIhGKKYq2AiB0SDg8LeM8z3rxgLzFyGtxBlLJnaKo5TeDIAI9vAsLiyq5Agjd//////////8BEAEaDDg0MDE3NzU5MjM5MCIMp5bx9AUmXH6vYwBzKo0CIh1Omd00vJChFMsh31tVqpS+ET/uwylLw54CR8di7h8NuZk8xmToERnJl/tq13pUDyFlwW9v9lgOtDhgo5rpOA4qAV8eCmbukWTp5j+5Y/5twAgGWvrZrgqeErnkjd/QmV5oiyVrxvcOITNe9Lw74JTSxDgvEvsPYivyu3+TFrBus0BbwrUoXc6Mh9lqzV0hKdzrf9cI7IfQMAWaXS9pDwkkY/k0/7dmACtm7EfhqaBE18o4bYYj1mP+VHph1+qytpYQA2LDM8DxgAunKvRzjSHFW0HhGAKQzAkmyXitP76D4eK2hHaQM2wdnV13buxUWa1XOWL2vixE6IJNw/w15dYLJAo8xTCHPFHn+AAw3orqsQY6ngE/XlKIm71p6x01o7srEIhtgqTNKhWODC78reKMZJy2DiHN7KzjyxZeV+0HHlWDz/cUvlL1K+HcwHKkY5F5WQ3P30h0N2QTaJlDvEVxrtj1ZG6xuBA37qyKuOb8CsRPTu24Ui8Ta3bbAUlZjV67JiiDdtEO46x3GVhzqDxKHIhi+/0AGeWdntZnXFoOeZgExW8KqxzYzK0n/qmRrlhI+w==',#secrets.AWS_SESSION_TOKEN
                    )
s3_client = boto3.client('s3',
                    aws_access_key_id='ASIA4HHT4VBDAYLGL342',#secrets.AWS_ACCESS_KEY,
                    aws_secret_access_key='MY0uzXM+ooVxSZjdDtfVLpCJni3wMLWJcQjEG4mU',#secrets.AWS_SECRET_KEY,
                    aws_session_token='IQoJb3JpZ2luX2VjEIT//////////wEaCXVzLXdlc3QtMiJGMEQCIClIafBJZsVKXD8rVvR/BxK3NFvz1OahIdFTIhGKKYq2AiB0SDg8LeM8z3rxgLzFyGtxBlLJnaKo5TeDIAI9vAsLiyq5Agjd//////////8BEAEaDDg0MDE3NzU5MjM5MCIMp5bx9AUmXH6vYwBzKo0CIh1Omd00vJChFMsh31tVqpS+ET/uwylLw54CR8di7h8NuZk8xmToERnJl/tq13pUDyFlwW9v9lgOtDhgo5rpOA4qAV8eCmbukWTp5j+5Y/5twAgGWvrZrgqeErnkjd/QmV5oiyVrxvcOITNe9Lw74JTSxDgvEvsPYivyu3+TFrBus0BbwrUoXc6Mh9lqzV0hKdzrf9cI7IfQMAWaXS9pDwkkY/k0/7dmACtm7EfhqaBE18o4bYYj1mP+VHph1+qytpYQA2LDM8DxgAunKvRzjSHFW0HhGAKQzAkmyXitP76D4eK2hHaQM2wdnV13buxUWa1XOWL2vixE6IJNw/w15dYLJAo8xTCHPFHn+AAw3orqsQY6ngE/XlKIm71p6x01o7srEIhtgqTNKhWODC78reKMZJy2DiHN7KzjyxZeV+0HHlWDz/cUvlL1K+HcwHKkY5F5WQ3P30h0N2QTaJlDvEVxrtj1ZG6xuBA37qyKuOb8CsRPTu24Ui8Ta3bbAUlZjV67JiiDdtEO46x3GVhzqDxKHIhi+/0AGeWdntZnXFoOeZgExW8KqxzYzK0n/qmRrlhI+w==',#secrets.AWS_SESSION_TOKEN
                    )

def start_scraping(patent_no, patent_url, bucket_name):
    
    # Use requests to fetch web content
    try:
        response = requests.get(patent_url)
    except:
        file_name = f"/failed_patent_no.txt"
        with open(file_name, 'w') as file:
            f_result = str(patent_no) + ","
            file.write(f_result)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Retrieve the content
        content = response.text
        
        # Create a filename and specify the /tmp directory
        file_name = f"/{patent_no}.txt"
        
        # Create a file object in the /tmp directory
        with open(file_name, 'w') as file:
            file.write(content)
        
        # Upload the file to S3
        s3.meta.client.upload_file(file_name, bucket_name, patent_no + ".txt")
        
        # Delete the temporary file after uploading
        os.remove(file_name)
    else:
        print(f"Failed to retrieve content from {patent_url}, status code: {response.status_code}")


# Specify the S3 bucket name
bucket_name = 'patent-bucket-raw'

# Get the list of patent numbers and URLs from the event

data = pd.read_csv('all_patents_link.csv')

data.dropna(subset=['id', 'result link'], inplace=True)

patent_nos = data['id'].tolist()

patent_urls = data['result link'].tolist()

# Iterate through the list of patent_nos and patent_urls
for patent_no, patent_url in zip(patent_nos, patent_urls):
    # Call start_scraping function for each pair of patent_no and patent_url
    start_scraping(patent_no, patent_url, bucket_name)
    time.sleep(2)  # Pause for 2 seconds between requests