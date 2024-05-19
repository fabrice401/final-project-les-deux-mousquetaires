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
        return None
    
    timeline_tags = soup.find_all('dd', itemprop='events')
    timeline_dict = {tag.find('time', itemprop='date').text.strip(): tag.find('span', itemprop='title').text.strip() for tag in timeline_tags}
    
    cited_by_tags = soup.find_all('tr', itemprop='forwardReferencesOrig')
    cited_by_dict = {tag.find('span', itemprop='publicationNumber').text.strip(): tag.find('span', itemprop='assigneeOriginal').text.strip() for tag in cited_by_tags if tag.find('span', itemprop='publicationNumber').text.strip().startswith('US')}
    
    legal_events_tags = soup.find_all('tr', itemprop='legalEvents')
    legal_events_dict = {tag.find('time', itemprop='date').text.strip(): tag.find('td', itemprop='title').text.strip() for tag in legal_events_tags if tag.find('td', itemprop='code').text.strip().startswith(('AS', 'PS'))}
    
    df = pd.DataFrame({
        'id': [file_id],
        'abstract': [abstract],
        'classification': [', '.join(classifications)],
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
    
    BUCKET_NAME = 'patent-bucket-raw-sam' 
    AWS_ACCESS_KEY_ID = 'ASIAR6SRDSVDDYLU3FHW'
    AWS_SECRET_ACCESS_KEY = 'DWVV19uwa/CBILqlbVYD2p+Scg5YuxprGVD4JtW7'
    AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjEKP//////////wEaCXVzLXdlc3QtMiJGMEQCIH7DJ7nbKxwq13i1hQ4nPQ5MfHJlxvpLGVjmfXGcn8plAiB2HdYmSTsNDiZyFYrzLJG/LNCHZ3NtFknMfH/qeVtl7iqvAggcEAAaDDEzNDM4NzgzMjEzNCIMCI1HwDWOwn2YCXo7KowCsLeflIjRms9VPH1wzM4PIW5V24hlBNAgYv4ziZOGK+oiWht8By3JrS2oJ3+uDgrxn+TPOMdKl6Vpn7LGgwhfL8sXj0NSqetz46P/7ICR3VU9c7IgxT37fS86w++mZW/qJIBUfvzkg09AucjPTHmoBKV64f6oO9IsQsInda4/99dnhNuHnJVDzoFYEPEf1zC1L4RqHFtEHgb27TEMHSpTh+hbNUEN6ZmukUP2NDNFZSSXrxVxtuG5ogawAKWcd4EW3U/cS5uwWwVs+ZjARqP/C0H9QXdleGrCZFstsGDZkHXfFHwKvYUevaV/jmTaG6Gy3yFpuzjmoR5hfrR5CusCTUgQbfVpg1gVI3MS3DC1mKmyBjqeAW1gqUikkSWutKa8m+XjW8cSrWUW0bhuXh+dcOmlW4mXVYpNETKCLl70IdPK+ZyCFy+d06pKuft5grnNd1x+tUtD0sW1PsfCCtiIXP+9M9Ej0BEDgaZ4u66CNx4oYRA9ArnP5VEOpK9ksXFElltSuKyvmb0kIH82jtm8QEGN5Z8Wv3tyc6FdYH4YAbm+9x2q0CksRmv5Rh8/VDJSQyX8'
    
    s3_client = boto3.client('s3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID, 
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        aws_session_token=AWS_SESSION_TOKEN
                        )
    
    checkpoint_dir = './checkpoint/'
    
    if rank == 0:
        print("Downloading file keys...")
        file_keys = download_and_process_files(s3_client, BUCKET_NAME)
        print(f"Total files to process: {len(file_keys)}")
    else:
        file_keys = None
    
    file_keys = comm.bcast(file_keys, root=0)
    
    files_per_process = len(file_keys) // size
    start_idx = rank * files_per_process
    end_idx = start_idx + files_per_process
    if rank == size - 1:
        end_idx = len(file_keys)
    
    files_to_process = file_keys[start_idx:end_idx]
    
    print(f"Process {rank} processing {len(files_to_process)} files...")
    
    local_df, processed_keys = load_checkpoint(checkpoint_dir, rank)

     # Calculate the set difference to exclude already processed files
    files_to_process = set(files_to_process) - set(processed_keys)
    
    for file_key in files_to_process:
        df = process_file_from_s3(s3_client, BUCKET_NAME, file_key)
        if df is not None:
            local_df = pd.concat([local_df, df], ignore_index=True)
            processed_keys.append(file_key)
        if len(processed_keys) % 100 == 0:
            save_checkpoint(local_df, processed_keys, checkpoint_dir, rank)
            print(f"Process {rank} saved checkpoint with {len(processed_keys)} processed files.")
    
    save_checkpoint(local_df, processed_keys, checkpoint_dir, rank)
    print(f"Process {rank} finished processing. Final checkpoint saved.")
    
    all_dfs = comm.gather(local_df, root=0)
    
    if rank == 0:
        print("Combining all DataFrames...")
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.to_csv('patent_data.csv', index=False)
        print("Data saved to CSV successfully")
