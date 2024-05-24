import pandas as pd
import os
from bs4 import BeautifulSoup

def save_to_csv(dfs, counter):
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv(f'patent_data_{counter//5000}.csv', index=False, encoding='utf-8-sig')
        print(f"Saved {len(dfs)} records to patent_data_{counter//5000}.csv")
        return []
    return dfs

# Create an empty list to store the data frames
dfs = []
counter = 0 

# loop through all files in the raw folder
folder_path = 'raw'
for file_name in os.listdir(folder_path):
    if file_name.endswith('.txt'):
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, 'r', encoding='utf-8-sig') as file:
            # get the file id
            file_id = os.path.splitext(file_name)[0]
            
            # read the file content
            content = file.read()
            soup = BeautifulSoup(content, 'html.parser')
            
            # get abstract
            abstract_tag = soup.find('section', itemprop='abstract')
            abstract = abstract_tag.find('div', itemprop='content').text.strip()
            
            # get classification
            classification_tags = soup.find_all('span', itemprop='Code')
            try:
                classifications = [tag.text.strip() for tag in classification_tags if len(tag.text.strip()) > 4]
                classifications = list(set(classifications))  # delete duplicates
            except:
                classifications = []
            
            if len(classifications) == 0:
                continue

            # get timeline
            timeline_tags = soup.find_all('dd', itemprop='events')
            timeline_dict = {}
            for timeline_tag in timeline_tags:
                try:
                    date = timeline_tag.find('time', itemprop='date').text.strip()
                    event = timeline_tag.find('span', itemprop='title').text.strip()
                    timeline_dict[date] = event
                except:
                    timeline_dict = {}
            
            
            # get cited by table (global)
            cited_by_tags = soup.find_all('tr', itemprop='forwardReferencesOrig')
            cited_by_dict = {}
            for cited_by_tag in cited_by_tags:
                try:
                    publication_number = cited_by_tag.find('span', itemprop='publicationNumber').text.strip()
                    assignee = cited_by_tag.find('span', itemprop='assigneeOriginal').text.strip()
                    cited_by_dict[publication_number] = assignee
                except:
                    cited_by_dict = {}


            # Create a DataFrame for the current file and append it to the list
            df = pd.DataFrame({
                'id': [file_id],
                'abstract': [abstract],
                'classification': [', '.join(classifications)],
                'timeline': [str(timeline_dict)],
                'citedby': [str(cited_by_dict)]
            })
            dfs.append(df)
            counter += 1

            # Save to CSV every 5000 files
            if counter % 5000 == 0:
                dfs = save_to_csv(dfs, counter)


# Concatenate all DataFrames in the list into one DataFrame
final_df = pd.concat(dfs, ignore_index=True)

# Save the DataFrame as a CSV file
final_df.to_csv('patent_data.csv', index=False, encoding='utf-8-sig')

print("Data saved to CSV successfully")
