import boto3
import pandas as pd
from bs4 import BeautifulSoup
from mpi4py import MPI
import os

# No prefix to list all files at the base level in s3 bucket (default setting)
def download_and_process_files(s3_client, bucket_name, s3_prefix=''):
    file_keys = []
    continuation_token = None

    # Deal with the case that list_objects_v2 can only retrieve 1,000 objects in the s3 bucket
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

        file_keys.extend([obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.txt')])

        if response.get('IsTruncated'):  # If there are more keys to retrieve
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    
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
    # Configure MPI settings
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    print(f"Process {rank} starting...")
    
    # Define constants (change based on individual circumstances)
    BUCKET_NAME = 'patent-bucket-raw-sam' 
    AWS_ACCESS_KEY_ID = 'ASIAR6SRDSVDAHMWVQFY'
    AWS_SECRET_ACCESS_KEY = 'j6bk43Yh7S6l6XSIcXF/dQom97S4yM/IGaFDRJuP'
    AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjEHoaCXVzLXdlc3QtMiJHMEUCIQChQ71EcEP2UJBKl+EbqcG4Y5BSErUeQPC4sSUNinBCSQIgdDMFRO4Rf7TFeupMYYH52DeKlhesqxCuA4lBl/Bor3AquAII4///////////ARAAGgwxMzQzODc4MzIxMzQiDOMx8a9AfjebSR1wYCqMAm95trNaGggtDr8OO6h04JW3BJK9SrdPWSdHrNSpcRPSObYsicmo8+HgFx/9+LHTNXt6IgRLmkRbol0HTzXqEfK4c+2man4NDv9dhMPO01dzoVpanfcixX0MPRjSIQORD3jKmJCZtV88+XGJLs+97HiwWE4sukTGRsKfKGqCAUvS7FtpYrgPNBKYMurYxc5GqKxRjQxD92k6F1Bw/T4igO13sydA4aXvgrhDtbD3gqsc6BmsFD2xxHkfN6vvs4QJBh/hcgaKmGQesGYfwJ7GbxGAUYNRMZ01q0CmD1AjKdEMRzIef8G1AVgM1wLfnLpq+5IM8/xBooCHMsv/gk6fEasmAqA7NsZErMM1UkAw7p6gsgY6nQHJmN9OYpDM5W9JUiqrUeTC8kbad6F3ehzQurWamresW4LuMartqqsuBpvKn/Fc4XaVGFElQmcCrP8KVAlxnQDl5E5hbAKBcltnM+lbT4qmVWr9VMSDXMiuy4OOkgdljXQsDZO9T9s6Pj6epHvdkKUkkg7xpnFuG9T+zwqiynp75yvv9+ZsgHrBGJ+gJMRqg/tgNNkY+7nq5oYgxRvU'
    
    # Initialize S3 client
    s3_client = boto3.client('s3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID, 
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        aws_session_token=AWS_SESSION_TOKEN
                        )
    
    if rank == 0:
        print("Downloading file keys...")
        file_keys = download_and_process_files(s3_client, BUCKET_NAME)
        print(f"Total files to process: {len(file_keys)}")
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
    
    print(f"Process {rank} processing {len(files_to_process)} files...")
    
    # Process assigned files and create local DataFrames
    local_dfs = []
    for file_key in files_to_process:
        print(f"Process {rank} processing file {file_key}...")
        df = process_file_from_s3(s3_client, BUCKET_NAME, file_key)
        if df is not None:
            local_dfs.append(df)
    
    print(f"Process {rank} finished processing files.")
    
    # Combine local DataFrames into one
    local_df = pd.concat(local_dfs, ignore_index=True) if local_dfs else pd.DataFrame()
    
    # Gather all DataFrames at the root process
    all_dfs = comm.gather(local_df, root=0)
    
    if rank == 0:
        print("Combining all DataFrames...")
        # Combine all DataFrames into the final DataFrame
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.to_csv('patent_data.csv', index=False)
        print("Data saved to CSV successfully")
