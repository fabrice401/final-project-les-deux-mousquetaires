import requests
import time
import pandas as pd


def start_scraping(patent_no, patent_url):
    
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
        file_name = f"raw/{patent_no}.txt"
        
        # Create a file object in the /tmp directory
        with open(file_name, 'w') as file:
            file.write(content)
    else:
        print(f"Failed to retrieve content from {patent_url}, status code: {response.status_code}")


# Get the list of patent numbers and URLs from the event
n = 0
data = pd.read_csv('all_patents_link.csv')
data = data.iloc[n:]
data.dropna(subset=['id', 'result link'], inplace=True)

patent_nos = data['id'].tolist()

patent_urls = data['result link'].tolist()

# Iterate through the list of patent_nos and patent_urls
for patent_no, patent_url in zip(patent_nos, patent_urls):
    # Call start_scraping function for each pair of patent_no and patent_url
    start_scraping(patent_no, patent_url)
    time.sleep(2)  # Pause for 2 seconds between requests