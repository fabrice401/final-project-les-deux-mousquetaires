# final-project-les-deux-mousquetaires
This is the final project for MACS 30123 course Large-Scale Computing for the Social Sciences owned by Guankun Li and Tianyue Cong. In this project, we utilized high performance computing techniques to scrape information about patents in the field of artifical intelligence (between 2019 and 2023) and conduct large-scale analysis, including clustering and network analysis, to explore the research (patent) trend in the field of artifical intelligence and the citation patterns during this period.

To do:

Canvas Repo Requirements: 
1. describing a social science research problem, justification of the importance of using scalable computing methods to solve it, as well as a description of the large-scale computing methods you employ in the project (1000 words minimum);
2. state the responsibilities of each group member in your README.

## Scraping
We chose to scrape the patents related to artifical intelligence from 2019 to 2023 using [Google Patents](https://patents.google.com/). We used midway3 to download a total of 382071 individual patents related to artifical intelligence (see [`download_patent_midway.py`](scraping/download_patent_midway.py) file). 

Underlying this python script, we utilized `selenium` package to dyanmically **search** and **download** relevant patent records to `all_patents_link.csv` file on [shared Google Drive](https://drive.google.com/drive/u/0/folders/1WVNa82HSAvxmRaRiNh5k4_ZN-g6WbEua?ths=true). See the [figure of example](screenshots/Selenium_Scraping.png) below: 

![](screenshots/Selenium_Scraping.png)

Specifically, after initializng the selenium drive, we used to following code to search for US patents with the keywords of artificial and intelligence during the time period between after_date and before_date (in our case, 2019.1.1 - 2024.1.1):
```python
url = f'https://patents.google.com/?q=(artificial+intelligence)&country=US&before=publication:{before_date}&after=publication:{after_date}&language=ENGLISH&type=PATENT'
```

Afterwards, we waited for the links (for each patent) to be clickable and then clicked them to download the csv file:
```python
# Download csv files of patent data for each month
download_css_selector = "a[href*='download=true'] iron-icon[icon='icons:file-download']"
# Wait for the link to be clickable and click it
link = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.CSS_SELECTOR, download_css_selector))
)
link.click()
# Wait for files to be downloaded
time.sleep(15)
```

As shown in the `all_patents_link.csv` file on shared Google Drive, the csv file stores (Google Patents) information about each patent and, more importantly, it stores the URL link to detailed information about the patent on Google Patents. The specific schema of `all_patents_link.csv` file summarized as below:
| Column Name                     | Description                                                                                       |
|---------------------------------|---------------------------------------------------------------------------------------------------|
| `id`                            | The unique identifier of the patent, formatted with hyphens (e.g., US-10171659-B2).              |
| `title`                         | The title of the patent.                                                                          |
| `assignee`                      | The entity to which the patent is assigned (e.g., a company or individual).                      |
| `inventor/author`               | The names of the inventors or authors of the patent.                                              |
| `priority date`                 | The date on which the patent application was first filed.                                         |
| `filing/creation date`          | The date on which the patent application was officially filed or created.                        |
| `publication date`              | The date on which the patent was published.                                                      |
| `grant date`                    | The date on which the patent was granted.                                                        |
| `result link`                   | The URL link to the detailed information about the patent on Google Patents.                      |
| `representative figure link`    | The URL link to the representative figure or image of the patent.                                |

We then used `request` package to retreive the details (e.g., paper abstract, classification, citation records) of each patent based on the `result link` column in `all_patents_link.csv` file above (see an example of webpage accessed from the `result link`):

![](screenshots/Example_Patent_Detail_Page.png)

As shown in [`scrape_each_patent_linux_local.py` file](scraping/scrape_each_patent_linux_local.py), we scraped text of `request` responses and uploaded individual text files to aws s3 bucket (given the large file size of all patent records combined). 

The strategic choice of scraping twice (one using `selenium` and the other using `request`) is due to the fact that it is very likely to be detected as a bot when clicking the links to patent details (given the very large number of patents). Although we tried writing aws lambda functions to scrape patent details, the 4-hour limit of our aws account makes it cumbersome to restart aws lab and update aws credentials multiple times given the large number of patents to scrape.

## Data Cleaning
An important step after scraping is converting all patent records (stored on s3 bucket) to a csv file. To extract relevant information from the 382071 individual patents, we utlizied Message Passing Interface (MPI) to access all objects in our s3 buckets and extract information including patent id, patent abstract, classification, timeline, citation records, and legal events into a concatenated csv (see below):
```python
# Format of dataframe storing patent information
df = pd.DataFrame({
        'id': [file_id],
        'abstract': [abstract],
        'classification': classifications,
        'timeline': [str(timeline_dict)],
        'citedby': [str(cited_by_dict)],
        'legal': [str(legal_events_dict)]
    })
```
```python
# Gather all DataFrames at the root process
all_dfs = comm.gather(local_df, root=0)

if rank == 0:
    print("Combining all DataFrames...")
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df.to_csv('patent_data.csv', index=False)
    print("Data saved to CSV successfully")
```

For more details on this step, please refer to [`s3_download_txt_to_csv_mpi.py`](data_cleaning/s3_download_txt_to_csv_mpi.py) file (**NOTE**: we also included a version of python script that does not utilize parallel processing; see [`txt_to_csv.py`](data_cleaning/txt_to_csv.py) file). To perform this operation, we submitted the [`mpi.sbatch`](data_cleaning/mpi.sbatch) sbatch file to midway:
```bash
cd data_cleaning
sbatch mpi.sbatch
cd ..
```

After having this csv, we joined it with the `all_patents_link.csv` file to incorporate the finest details of each patent for later analysis. Named as `all_patent_info.csv`, this combined dataframe is saved on [shared Google Drive](https://drive.google.com/drive/u/0/folders/1WVNa82HSAvxmRaRiNh5k4_ZN-g6WbEua?ths=true)) and lays the foundation for our later analysis. 

## Clustering

Based on the elbow plot, the optimal number of clusters is determined by identifying the point where the decrease in the sum of squared errors (SSE) starts to slow down, forming an "elbow" shape. This point indicates diminishing returns in reducing SSE with the addition of more clusters. In your elbow plot, there is a noticeable "elbow" at around 11 clusters. Up to this point, the SSE decreases significantly as the number of clusters increases, indicating substantial improvements in cluster homogeneity. After 11 clusters, the rate of SSE reduction diminishes, suggesting that adding more clusters provides only marginal improvements.

From 2 to 11 clusters, there is a significant drop in SSE, which means that the data is being divided into more homogenous groups effectively. At 11 clusters, the plot shows a noticeable change in the slope, forming an "elbow." This indicates that 11 clusters is a turning point where adding more clusters does not significantly improve the model. Beyond 11 clusters, the SSE continues to decrease, but the rate of decrease slows down considerably. This suggests that the additional clusters are not providing substantial gains in reducing within-cluster variance. Choosing 11 as the optimal number of clusters strikes a balance between model complexity and clustering performance, ensuring meaningful and distinct groupings without overfitting the data. 

![](NLP/elbow_plot_keywords.png)


We analyzed each cluster by examining the top 50 keywords for each cluster. These keywords were then inputted into ChatGPT to generate a detailed description representing each cluster.

| Cluster | English Description                                       | Detailed  Description                                                                                           |
| ------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| 0       | Intelligent Data Processing and Machine Learning Systems  | This cluster includes systems and technologies focused on intelligent data processing and the application of machine learning algorithms to derive insights and automate decision-making processes. |
| 1       | 3D Point Cloud Processing and LiDAR Applications          | This cluster encompasses technologies related to the processing of 3D point cloud data and applications utilizing LiDAR for mapping, navigation, and environmental scanning. |
| 2       | Audio and Speech Processing Systems                       | This cluster contains systems and technologies for processing audio signals and speech, including speech recognition, synthesis, and audio enhancement. |
| 3       | Support Systems and Medical Devices                       | This cluster includes various support systems and medical devices designed to assist in healthcare delivery, patient monitoring, and diagnostic procedures. |
| 4       | File Management and Data Processing Systems               | This cluster focuses on technologies and systems for efficient file management, data storage, retrieval, and processing to support various applications. |
| 5       | Entity Recognition and Knowledge Graph Systems            | This cluster comprises systems that perform entity recognition and construct knowledge graphs to enable sophisticated data relationships and contextual understanding. |
| 6       | Wireless Communication and Device Control Systems         | This cluster involves technologies for wireless communication and systems for remote control and monitoring of various devices. |
| 7       | IoT Devices and Edge Computing                            | This cluster includes Internet of Things (IoT) devices and edge computing technologies that enable decentralized data processing close to the data source. |
| 8       | Semiconductor Devices and Material Technologies           | This cluster focuses on advancements in semiconductor devices and material technologies essential for developing new electronic components. |
| 9       | Task Management and Machine Learning Systems              | This cluster includes systems for managing tasks and integrating machine learning models to optimize workflow and enhance productivity. |
| 10      | Image Processing and Data Analysis Systems                | This cluster contains technologies and systems for processing images and performing data analysis to extract meaningful information and insights. |

