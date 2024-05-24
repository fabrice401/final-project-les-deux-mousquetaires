import os
import boto3

def download_and_process_files(s3_client, bucket_name, s3_prefix='raw33_38/', local_folder='raw'):
    continuation_token = None
    all_objects = []

    # Retrieve all objects in the bucket with the specified prefix
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=s3_prefix,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=s3_prefix
            )
        
        all_objects.extend(response.get('Contents', []))
        
        if response.get('IsTruncated'):  # If there are more keys to retrieve
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    # Sort objects by key
    sorted_objects = sorted(all_objects, key=lambda x: x['Key'])
    limited_objects = sorted_objects[9000:30000]
    
    # Create local folder if it does not exist
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)

    # Download .txt objects
    count = 0
    for i, obj in enumerate(limited_objects, start=1):
        key = obj['Key']
        if key.endswith('.txt'):
            file_name = key.split('/')[-1]  # Specify file name
            local_file_path = os.path.join(local_folder, file_name)  # Specify local file path
            s3_client.download_file(bucket_name, key, local_file_path)  # Download file
            
            count += 1
            if count % 1000 == 0:
                print(f"Downloaded {count} files...")

# Initialize S3 client
s3_client = boto3.client('s3',
                    aws_access_key_id='ASIA4HHT4VBDPHWJV7TB',#secrets.AWS_ACCESS_KEY,
                    aws_secret_access_key='vrUEWM/TEHJ1n7KvJAeAO+TlvHir0XLoknEKwq45',#secrets.AWS_SECRET_KEY,
                    aws_session_token='IQoJb3JpZ2luX2VjENv//////////wEaCXVzLXdlc3QtMiJHMEUCICitvbr2kmS3fQAbCfK4TZSxv3NwfpGyxvZLDd4UVm6hAiEAmb80j6Q+PMdPxns9nvXFRod7ncdB/C33IietQskAB7EqsAIIVBABGgw4NDAxNzc1OTIzOTAiDNk9mtoXqlE1DqE58SqNArjYNbFtDsa441ELkB2kNL/G1pFrjX2engR/kJjlO2VUg08QrIwO1VwNGAR2uyeHBHOovmoC0KUaZH6SfgykWQynFrrFTs4ej6Xwrw6RwCO6+C/IdzmKlqBLfNmIEGmD+xp5uRDueepqSV433g4IvNVVQNgSFg2PfUUM8Z7qEEMNSH65otGHXv8C6Yvm4qZCWHGGue1zvP4FyHKu0Y3cqERthDOrfioOxQRjwyNkxHrSeoZkr0B2Cl2/cwam3H2U49rXR2hS0WVJ1yUZXruFy8k1lolP3ea4Jwz9NEeGk1Epo5o7ALZsD/6sfzjw4eNh9bThH6M8mRcwYnYb4Oh7rHsJs4qb3wVxC+ATm9JVMNnGtbIGOp0B3VGmojZKTBua6dNIcitXWzfu15fJ+7+BGpNBHt3c5olEq07RKMMJGijhjXJ/mc1pt1V4meoh0yaAl3lvgcZ4fFM8wtWpgZdtb3w9H+fAXYKjTJOB5lYN/pFWRJc7s5wb8qwxKSs8eYXOvpbqiVphBZenw0bmsJHF2/VSNbKL6jok330azwgg34jirJM7duYee6/grNc1UuEx/ERLVA==',#secrets.AWS_SESSION_TOKEN
                    )

# Bucket name
BUCKET_NAME = 'patent-bucket-raw'

# Call the function to download .txt files from 'raw1_3' folder
download_and_process_files(s3_client, BUCKET_NAME, s3_prefix='raw33_38/', local_folder='raw')
