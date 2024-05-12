import pandas as pd
import os
from bs4 import BeautifulSoup

# Create an empty list to store the data frames
dfs = []

# loop through all files in the raw folder
folder_path = 'raw'
for file_name in os.listdir(folder_path):
    if file_name.endswith('.txt'):
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, 'r', encoding='utf-8') as file:
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
            classifications = [tag.text.strip() for tag in classification_tags if len(tag.text.strip()) > 4]
            classifications = list(set(classifications))  # delete duplicates
            if len(classifications) == 0:
                continue

            # get timeline
            timeline_tags = soup.find_all('dd', itemprop='events')
            timeline_dict = {}
            for timeline_tag in timeline_tags:
                date = timeline_tag.find('time', itemprop='date').text.strip()
                event = timeline_tag.find('span', itemprop='title').text.strip()
                timeline_dict[date] = event
            
            # get cited by table
            cited_by_tags = soup.find_all('tr', itemprop='forwardReferencesOrig')
            cited_by_dict = {}
            for cited_by_tag in cited_by_tags:
                publication_number = cited_by_tag.find('span', itemprop='publicationNumber').text.strip()
                if not publication_number.startswith('US'):
                    continue  # if not a US patent, skip
                assignee = cited_by_tag.find('span', itemprop='assigneeOriginal').text.strip()
                cited_by_dict[publication_number] = assignee

            # get legal events table
            legal_events_tags = soup.find_all('tr', itemprop='legalEvents')
            legal_events_dict = {}
            for legal_event_tag in legal_events_tags:
                code = legal_event_tag.find('td', itemprop='code').text.strip()
                if (code.startswith('AS') and len(code) == 4) or code.startswith('PS'):
                    date = legal_event_tag.find('time', itemprop='date').text.strip()
                    title = legal_event_tag.find('td', itemprop='title').text.strip()
                    legal_events_dict[date] = title

            # Create a DataFrame for the current file and append it to the list
            df = pd.DataFrame({
                'id': [file_id],
                'abstract': [abstract],
                'classification': [', '.join(classifications)],
                'timeline': [str(timeline_dict)],
                'citedby': [str(cited_by_dict)],
                'legal': [str(legal_events_dict)]
            })
            dfs.append(df)

# Concatenate all DataFrames in the list into one DataFrame
final_df = pd.concat(dfs, ignore_index=True)

# Save the DataFrame as a CSV file
final_df.to_csv('patent_data.csv', index=False)

print("Data saved to CSV successfully")
