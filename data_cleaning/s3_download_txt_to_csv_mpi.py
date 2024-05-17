import boto3
import pandas as pd
from bs4 import BeautifulSoup
from mpi4py import MPI
import os

# No prefix to list all files at the base level in s3 bucket (default setting)
def download_and_process_files(s3_client, bucket_name, s3_prefix=''):
    # List objects in the specified S3 bucket and prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    
    # Collect file keys
    file_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.txt')]
    
    return file_keys

def process_file_from_s3(s3_client, bucket_name, file_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    content = response['Body'].read().decode('utf-8')
    
    # Process the file content
    file_id = os.path.splitext(os.path.basename(file_key))[0]
    soup = BeautifulSoup(content, 'html.parser')
    
    # Get abstract
    abstract_tag = soup.find('section', itemprop='abstract')
    abstract = abstract_tag.find('div', itemprop='content').text.strip() if abstract_tag else ''
    
    # Get classification
    classification_tags = soup.find_all('span', itemprop='Code')
    classifications = [tag.text.strip() for tag in classification_tags if len(tag.text.strip()) > 4]
    classifications = list(set(classifications))  # remove duplicates
    
    if not classifications:
        return None
    
    # Get timeline
    timeline_tags = soup.find_all('dd', itemprop='events')
    timeline_dict = {tag.find('time', itemprop='date').text.strip(): tag.find('span', itemprop='title').text.strip() for tag in timeline_tags}
    
    # Get cited by table
    cited_by_tags = soup.find_all('tr', itemprop='forwardReferencesOrig')
    cited_by_dict = {tag.find('span', itemprop='publicationNumber').text.strip(): tag.find('span', itemprop='assigneeOriginal').text.strip() for tag in cited_by_tags if tag.find('span', itemprop='publicationNumber').text.strip().startswith('US')}
    
    # Get legal events table
    legal_events_tags = soup.find_all('tr', itemprop='legalEvents')
    legal_events_dict = {tag.find('time', itemprop='date').text.strip(): tag.find('td', itemprop='title').text.strip() for tag in legal_events_tags if tag.find('td', itemprop='code').text.strip().startswith(('AS', 'PS'))}
    
    # Create a DataFrame for the current file
    df = pd.DataFrame({
        'id': [file_id],
        'abstract': [abstract],
        'classification': [', '.join(classifications)],
        'timeline': [str(timeline_dict)],
        'citedby': [str(cited_by_dict)],
        'legal': [str(legal_events_dict)]
    })
    
    return df

if __name__ == '__main__':
    # Configure MPI setting
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Define constants (change based on individual circumstances)
    BUCKET_NAME = 'patent-bucket-raw-sam' 
    AWS_ACCESS_KEY_ID = 'ASIAR6SRDSVDHVPQDUP4'
    AWS_SECRET_ACCESS_KEY = 'EiPYsQYkBAnjSKda5edRtDSeF49aIBE2YQDUKxjY'
    AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjEHYaCXVzLXdlc3QtMiJIMEYCIQC1KAAr4bDz7w5wdD+L8VQs9+llPIFcCyFTy80rjxrbDAIhAIDwns+hioAJNgecOig1Uzu7BfQZgYbVabxfOCe05N7OKrgCCN///////////wEQABoMMTM0Mzg3ODMyMTM0IgzSLljGeo+9lZY67scqjAIljrwc/Ev7b3uBWFEvWbSV3XQEAT0P0E4VGnPbK7FpRa0lLVFKSDaPbPO0Qn6YJoF4vbr55txLmWd8WYNXcdzP2Fmql0eQGewOzJTS0OdocxwJ41IeICGIpZsxzzCzyyZSpqAAqqYs6UUk6k38VU6GfBiGSlH3o63Rz2a+L9J3RRrYohGsIFcbgwRtamyg5TkjPJRDvwL1NU33kt95S8av3h6tj/cnw6IdQSQKbMWKZL7d0Zg9depZR0IFyMYN1/jLM5k1RwqFs2ei8MTWZFbfM2ZwVXIOJapTX/Gt8ruQPqBliWmzU/rGtQOMHor84W04MZr8HhKWQ20jmlxuMuZiCIEQpZ8Kp1elHyrlMJeqn7IGOpwBlI8kuDogWRQ59ONuGYKbNtxc/anb/X571XeTvFXAWAaf1nsx9TOfV5ud8SEIvj99/DHRI46TgoDw85nJbqpR1iHxlf2K0B3qxz6cP/hUVDsnU0wZQZYXIZoBTi5VHTQ5f/SqdFUGRfz5biUrY5W4gvIwjK0ZjVgaIB7KugovVQ7t7J6QSaFDOKGGA/1GAL+8iFbPgFiB2OfbT8hE'
    
    # Initialize S3 client
    s3_client = boto3.client('s3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID, 
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        aws_session_token=AWS_SESSION_TOKEN
                        )
    
    if rank == 0:
        file_keys = download_and_process_files(s3_client, BUCKET_NAME)
    else:
        file_keys = None
    
    # Broadcast the file keys to all processes
    file_keys = comm.bcast(file_keys, root=0)
    
    # Distribute files to processes
    files_per_process = len(file_keys) // size
    start_idx = rank * files_per_process
    end_idx = start_idx + files_per_process
    if rank == size - 1:
        end_idx = len(file_keys)
    
    files_to_process = file_keys[start_idx:end_idx]
    
    # Process assigned files and create local DataFrames
    local_dfs = []
    for file_key in files_to_process:
        df = process_file_from_s3(BUCKET_NAME, file_key, s3_client)
        if df is not None:
            local_dfs.append(df)
    
    # Combine local DataFrames into one
    local_df = pd.concat(local_dfs, ignore_index=True) if local_dfs else pd.DataFrame()
    
    # Gather all DataFrames at the root process
    all_dfs = comm.gather(local_df, root=0)
    
    if rank == 0:
        # Combine all DataFrames into the final DataFrame
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.to_csv('patent_data.csv', index=False)
        print("Data saved to CSV successfully")