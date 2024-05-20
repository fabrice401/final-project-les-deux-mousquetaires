import boto3
import pandas as pd
from bs4 import BeautifulSoup
from mpi4py import MPI
import os

def download_and_process_files(s3_client, bucket_name, s3_prefix=''):
    file_keys = []
    continuation_token = None

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

        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    
    return file_keys

def process_file_from_s3(s3_client, bucket_name, file_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    content = response['Body'].read().decode('utf-8')
    
    file_id = os.path.splitext(os.path.basename(file_key))[0]
    soup = BeautifulSoup(content, 'html.parser')
    
    abstract_tag = soup.find('section', itemprop='abstract')
    abstract = abstract_tag.find('div', itemprop='content').text.strip() if abstract_tag else ''
    
    classification_tags = soup.find_all('span', itemprop='Code')
    classifications = [tag.text.strip() for tag in classification_tags if len(tag.text.strip()) > 4]
    classifications = list(set(classifications))
    
    if not classifications:
        classifications = None
    else:
        classifications = [', '.join(classifications)]
    
    timeline_tags = soup.find_all('dd', itemprop='events')
    timeline_dict = {tag.find('time', itemprop='date').text.strip(): tag.find('span', itemprop='title').text.strip() for tag in timeline_tags}
    
    cited_by_tags = soup.find_all('tr', itemprop='forwardReferencesOrig')
    cited_by_dict = {tag.find('span', itemprop='publicationNumber').text.strip(): tag.find('span', itemprop='assigneeOriginal').text.strip() for tag in cited_by_tags if tag.find('span', itemprop='publicationNumber').text.strip().startswith('US')}
    
    legal_events_tags = soup.find_all('tr', itemprop='legalEvents')
    legal_events_dict = {tag.find('time', itemprop='date').text.strip(): tag.find('td', itemprop='title').text.strip() for tag in legal_events_tags if tag.find('td', itemprop='code').text.strip().startswith(('AS', 'PS'))}
    
    df = pd.DataFrame({
        'id': [file_id],
        'abstract': [abstract],
        'classification': classifications,
        'timeline': [str(timeline_dict)],
        'citedby': [str(cited_by_dict)],
        'legal': [str(legal_events_dict)]
    })
    
    return df

def save_checkpoint(df, processed_keys, checkpoint_dir, rank):
    checkpoint_file = os.path.join(checkpoint_dir, f'checkpoint_rank_{rank}.csv')
    df.to_csv(checkpoint_file, index=False)
    processed_keys_file = os.path.join(checkpoint_dir, f'processed_keys_rank_{rank}.txt')
    with open(processed_keys_file, 'w') as f:
        for key in processed_keys:
            f.write(f"{key}\n")

def load_checkpoint(checkpoint_dir, rank):
    checkpoint_file = os.path.join(checkpoint_dir, f'checkpoint_rank_{rank}.csv')
    if os.path.exists(checkpoint_file):
        df = pd.read_csv(checkpoint_file)
    else:
        df = pd.DataFrame()
    processed_keys_file = os.path.join(checkpoint_dir, f'processed_keys_rank_{rank}.txt')
    if os.path.exists(processed_keys_file):
        with open(processed_keys_file, 'r') as f:
            processed_keys = [line.strip() for line in f]
    else:
        processed_keys = []
    return df, processed_keys

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    print(f"Process {rank} starting...")
    
    # Define some constants Change based on individual circumstances
    BUCKET_NAME = 'patent-bucket-raw-sam'
    AWS_ACCESS_KEY_ID = 'ASIAR6SRDSVDE7B5UNE2'
    AWS_SECRET_ACCESS_KEY = 'wSaP1cSFVHQcHPymFN0iba+uIOiLSSR/uy0OXCF2'
    AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjELf//////////wEaCXVzLXdlc3QtMiJHMEUCIQC9NvYYf12BSLsxp/8CpoxDkzjADde/mwGxP6xC7goaQgIgX4LYc54V2kN3y+MBhWPUkIEI0yniCxLIgQGl0nfeYhcqrwIIMBAAGgwxMzQzODc4MzIxMzQiDJA1rridtNaRA7NGUSqMAicGj7S1LRy1ge6sAq5H4fZbVqo/Ti9qmai353cnHDKuKCUnZNqHN9COo6kkG1wElb9uogAQbN1LxqHCSEKeTxb5N0zTWUcJQ+HsbDadB9rGVRru7MBSrqwnbq2iLFncB9TytjJPp3Vr0J/vY1wrG/B8vAqE6o/2hfoAN3YWV00uvStxfCRKxyLe46ayM6C8oFCAIx2Gbf3+jJ0WHnqLNlO0xrotkrUgTGkhH+5OjCOeipzc56xnrRamhVa+tYDsj/nraC0yZDOKQ7MzbKYN/ftJlpum5FMuOx5FvrRFOXXYvruMOUj+0mKR6SHmRhCaK3PiOYoWEdUKFeratzjYuyzrfwqJzwe+NOEix5Aw/MqtsgY6nQGu3+4s1HvuSv+8it3PHLXehLWlm/gsRUfwY1aZKfKICxinWZk0GBbtDdplsl4MPmnvCZx3su4Ybpx08eJ0ERcglHFRaxmecxavBfYRjyqw51KgSJmWhqCM6PCynwtEyyXBGax1lKVNZgTzGoNVJPtlreMhiDHnej++zQTacD+tdofEi3pJgSQsazAnXuwxC+jeDGR+asx4+3i5bYv+'
    CHECKPOINT_DIR = './checkpoint/'
    
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
    
    file_keys = comm.bcast(file_keys, root=0)
    
    local_df, processed_keys = load_checkpoint(CHECKPOINT_DIR, rank)

    # Calculate the set difference to exclude already processed files
    files_to_process = list(set(file_keys) - set(processed_keys))

    # Determine the chunk of work for each process
    chunk_size = len(files_to_process) // size
    start_index = rank * chunk_size
    end_index = start_index + chunk_size if rank != size - 1 else len(files_to_process)
    files_to_process_chunk = files_to_process[start_index:end_index]

    print(f"Process {rank} processing {len(files_to_process_chunk)} files...")

    for file_key in files_to_process_chunk:
        df = process_file_from_s3(s3_client, BUCKET_NAME, file_key)
        local_df = pd.concat([local_df, df], ignore_index=True)
        processed_keys.append(file_key)

        if len(processed_keys) % 1000 == 0:
            save_checkpoint(local_df, processed_keys, CHECKPOINT_DIR, rank)
            print(f"Process {rank} saved checkpoint with {len(processed_keys)} processed files.")
    
    save_checkpoint(local_df, processed_keys, CHECKPOINT_DIR, rank)
    print(f"Process {rank} finished processing. Final checkpoint saved.")

    # Gather all DataFrames at the root process
    all_dfs = comm.gather(local_df, root=0)
    
    if rank == 0:
        print("Combining all DataFrames...")
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.to_csv('patent_data.csv', index=False)
        print("Data saved to CSV successfully")