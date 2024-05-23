# Large-Scale Analysis of Patents in Artificial Intelligence: Trends and Network Insights
This is the final project for MACS 30123 course Large-Scale Computing for the Social Sciences owned by Guankun Li and Tianyue Cong. In this project, we utilized high performance computing techniques to scrape information about patents in the field of artifical intelligence (between 2019 and 2023) and conduct large-scale analysis, including clustering and network analysis, to explore the research (patent) trend in the field of artifical intelligence and the citation patterns during this period.

## Responsibilities
- Guankun Li: Web scraping, Clustering & Trends
- Tianyue Cong: Web scraping, Network Analysis

## Research Question

1. **Are there distinct subfields within the domain of AI patents? If so, what are the characteristics of these subfields?**

2. **Which subfields of AI are more popular, and what trends do they exhibit? How might these subfields develop in the future?**

3. **What are the characteristics of the patent network formed through citations in the AI field? Do patents in different subfields exhibit distinct network properties?**

4. **TBD**

## Social Science Significance

This study holds significant social science importance in multiple ways, including the analysis of technological trends, the structure of innovation networks, and the impact on various socio-economic factors.

### Importance of Analyzing AI Patents

AI is rapidly transforming various industries, driving advancements in fields such as healthcare, finance, and neuroscience. Patents are a vital indicator of technological innovation, representing the forefront of research and development. By analyzing AI patents, we can gain insights into **Technological Trends and Innovation Dynamics**. For example, by examining the growth in patents related to AI subfields like image processing or natural language processing, we can identify which areas are experiencing rapid development and which are plateauing (Henderson, Jaffe, & Trajtenberg, 1998). This understanding can inform strategic investment decisions and policy formulations aimed at fostering innovation (OECD, 2015).

Identifying distinct subfields within AI patents allows for a more granular understanding of innovation. This classification can reveal the specific characteristics and focus areas within the broader AI domain, helping policymakers and researchers prioritize funding and support (Griliches, 1990). Such insights are crucial for tailoring education and training programs to meet the demands of emerging technological fields (National Academy of Sciences, 2017).

### Importance of Patent Network Analysis

Analyzing the citation network of AI patents provides insights into the structure and dynamics of knowledge dissemination. Citation networks illustrate how innovations build upon each other, highlighting influential patents and seminal technologies. This analysis is critical for several reasons:

1.**Knowledge Flow and Innovation Clusters**:
Citation networks reveal how knowledge flows between different entities and technological domains. Understanding these networks can identify innovation clusters and influential patents, guiding strategic decisions in research and development (Singh, 2005). This can also highlight potential collaborations and partnerships that can accelerate technological advancements (Breschi & Lissoni, 2001).

2.**Interdisciplinary Connections**:
AI is inherently interdisciplinary, often combining insights from computer science, engineering, and domain-specific knowledge. By examining citation patterns, we can uncover how different fields contribute to AI advancements and foster interdisciplinary collaboration (Narin, Hamilton, & Olivastro, 1997). This understanding can enhance the design of interdisciplinary research initiatives and funding programs (Porter & Rafols, 2009).

## Computational Challenges and Scalability

We encountered computational challenges, and to address these, we had to use scalable computing methods.

1. **Scraping and Data Storage**:
   - Our project involved scraping a large volume of raw text data, exceeding 200GB. To manage and store this data efficiently, we utilized AWS S3, which offers virtually unlimited storage capacity and automatic scalability. For processing this extensive dataset, we employed midway's mpi4py to enable parallel processing, ensuring that our data handling was both efficient and scalable. This approach allowed us to distribute the workload across multiple processors, significantly speeding up the data processing tasks.

2. **NLP and Clustering**:
   - Processing and analyzing the vast amount of textual data for NLP and clustering presented significant computational challenges. To handle this, we used PySpark, a powerful distributed computing framework. PySpark's ability to process large datasets in parallel across a cluster of machines made it ideal for our needs. This distributed approach ensured efficient utilization of computational resources and allowed us to scale our NLP and clustering computations as the dataset grew. PySpark also provided robust libraries for text processing and machine learning, facilitating complex analysis at scale.

3. **Network Analysis**:
   - Constructing and analyzing a network with over 500,000 nodes and edges required substantial computational power. We addressed this challenge by leveraging both PySpark and Dask. PySpark was used for the initial data processing and feature extraction, while Dask provided the necessary scalability for the detailed network analysis and visualization tasks. By combining these tools, we were able to compute network characteristics and generate visualizations efficiently, even with the large scale of our data. This hybrid approach ensured that our network analysis was both comprehensive and scalable.


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

Based on the elbow plot, the optimal number of clusters is determined by identifying the point where the decrease in the sum of squared errors (SSE) starts to slow down, forming an "elbow" shape. This point indicates diminishing returns in reducing SSE with the addition of more clusters. In your elbow plot, there is a noticeable "elbow" at around 12 clusters. Up to this point, the SSE decreases significantly as the number of clusters increases, indicating substantial improvements in cluster homogeneity. After 12 clusters, the rate of SSE reduction diminishes, suggesting that adding more clusters provides only marginal improvements.

From 2 to 12 clusters, there is a significant drop in SSE, which means that the data is being divided into more homogenous groups effectively. At 12 clusters, the plot shows a noticeable change in the slope, forming an "elbow." This indicates that 12 clusters is a turning point where adding more clusters does not significantly improve the model. Beyond 12 clusters, the SSE continues to decrease, but the rate of decrease slows down considerably. This suggests that the additional clusters are not providing substantial gains in reducing within-cluster variance. Choosing 12 as the optimal number of clusters strikes a balance between model complexity and clustering performance, ensuring meaningful and distinct groupings without overfitting the data. 

![](NLP_clustering/elbow_plot_keywords.png)


We analyzed each cluster by examining the top 50 keywords for each cluster. These keywords were then inputted into ChatGPT to generate a detailed description representing each cluster.

| Cluster | Cluster Name                         | Description                                      |
|---------|--------------------------------------|--------------------------------------------------|
| 0       | General Machine Learning and Systems | Focuses on general machine learning methods and system configurations, including data processing, system integration, model training, and control mechanisms. |
| 1       | Image Processing and Recognition     | Involves techniques for processing and recognizing images, including methods for image analysis, object detection, image enhancement, and medical imaging. |
| 2       | Document Management and Text Processing | Pertains to handling and analyzing text and documents, involving document retrieval, text classification, content extraction, and digital document management systems. |
| 3       | Sample Analysis and Training Models  | Covers methods for analyzing samples and training models, including data sampling, model training, image analysis, and statistical methods for data evaluation. |
| 4       | User Interaction and Content Management | Deals with user interfaces and content management systems, focusing on user data interaction, content delivery, media services, and user request handling. |
| 5       | Virtual Reality and Augmented Reality | Encompasses technologies related to virtual and augmented reality, including virtual environments, physical interaction, display systems, and augmented reality applications. |
| 6       | Resource Management and Communication | Involves management of resources and communication systems, including resource allocation, network management, wireless communication, and data transmission. |
| 7       | Authentication and Security          | Focuses on user authentication and security measures, including biometric identification, security protocols, access control, and data protection mechanisms. |
| 8       | Signal Processing and Wireless Communication | Pertains to processing signals and wireless communication technologies, including signal modulation, wireless networks, data transmission, and communication protocols. |
| 9       | Feature Extraction and Machine Learning | Involves extracting features and applying machine learning techniques, including feature engineering, neural networks, classification algorithms, and training models. |
| 10      | Financial Transactions and Payment Systems | Deals with handling financial transactions and payment systems, including transaction processing, payment methods, fraud detection, and blockchain technologies. |
| 11      | Audio Processing and Speech Recognition | Encompasses audio processing and speech recognition technologies, including signal processing, voice recognition, audio encoding, and speech-to-text systems. |

### **Wordcloud Analysis for Clustering Results**

To provide a clearer visualization of the contents of each cluster, we have generated word clouds for each of the 12 clusters. Each word cloud displays the most frequently occurring words in the abstracts assigned to the respective cluster. This visual representation helps in understanding the dominant themes and topics within each cluster. Below, we provide a general analysis of how these word clouds align with our naming conventions for each cluster, using a couple of clusters as examples to illustrate the consistency.

Across all clusters, the word clouds consistently reflect the main themes identified in the cluster names. For instance, clusters related to document management prominently feature terms like "document" and "text," while those focused on user interaction highlight words such as "user" and "content." This consistency demonstrates that our clustering and naming process effectively captures the key topics and technological areas represented in the patent abstracts.

![](NLP_clustering/wordcloud.png)

### Quantity and Quality of Patents

We further analyzed the AI patents in different clusters in terms of quantity and quality. Quantity is measured by the number of patents within each cluster, while quality is determined by the average number of citations per patent in each cluster. The results are presented in two bar charts: the first chart displays the number of patents per cluster, and the second chart shows the average quality of patents based on citations.

The first chart reveals significant disparities in the number of patents across clusters. Notably, "User Interaction and Content Management" and "Signal Processing and Wireless Communication" exhibit the highest patent counts, with "User Interaction and Content Management" leading by a substantial margin. This indicates a higher concentration of patents in these clusters, possibly reflecting areas of intense research and development activity. In contrast, clusters such as "Sample Analysis and Training Models," "Virtual Reality and Augmented Reality," and "Authentication and Security" have notably fewer patents, suggesting either niche fields or areas with less research focus.

The second chart, which shows the average quality of patents based on citations per patent, provides a different perspective. Interestingly, "Authentication and Security" stands out with the highest average quality, indicating that, despite having fewer patents, the patents in this cluster are highly influential. "Virtual Reality and Augmented Reality" and "Signal Processing and Wireless Communication" also demonstrate high average quality, suggesting that patents in these clusters are well-cited and possibly more impactful.

The combined analysis from both charts reveals a nuanced landscape. For example, while "User Interaction and Content Management" has the highest number of patents, its average quality is moderate, indicating a large volume but not necessarily high-impact patents. Conversely, "Authentication and Security," with fewer patents, shows exceptional quality, highlighting the importance of considering both quantity and quality for a comprehensive understanding of patent landscapes.

![](NLP_clustering/quan_quality.png)

## Analysis of AI Patent Trends

#### Removal of Cluster 0
Cluster 0, "General Machine Learning and Systems," was removed from the trend analysis because it is too broad and can be further subdivided into more specific categories. This broad categorization does not provide actionable insights into the particular advancements and trends within the various subfields of AI. By excluding Cluster 0, we can focus more accurately on specific technological developments and their trajectories.

#### Trend Analysis of the Top Three Clusters
The trend analysis focuses on the top three clusters with the highest number of patents over time, which are Cluster 8, Cluster 1, and Cluster 4.

1. **Cluster 8 (Signal Processing and Wireless Communication)**:
   - **Trend**: This cluster consistently has the highest number of patents. From Q1 2019 to Q4 2023, the number of patents increased from around 1500 to over 3500.
   - **Analysis**: The steady rise in patents indicates continuous innovation and significant interest in signal processing and wireless communication technologies. This could be driven by the growing demand for advanced communication systems, 5G technology, and the increasing importance of robust signal processing methods in various applications.

2. **Cluster 1 (Image Processing and Recognition)**:
   - **Trend**: Patents in this cluster also show a steady increase, though at a slightly slower rate compared to Cluster 8. The number of patents has grown consistently over the observed period.
   - **Analysis**: The upward trend highlights the ongoing advancements and importance of image processing and recognition technologies. These technologies are critical in numerous applications, including medical imaging, autonomous vehicles, and security systems, reflecting their broad applicability and the substantial research and development efforts in this area.

3. **Cluster 4 (User Interaction and Content Management)**:
   - **Trend**: The number of patents in this cluster has significantly increased, showing a robust upward trajectory from Q1 2019 to Q4 2023.
   - **Analysis**: The growth in patents related to user interaction and content management suggests that this area is becoming increasingly important as digital and smart technologies evolve. Innovations in user interfaces and content management systems are essential for enhancing user experience and managing the growing volumes of digital content effectively.

![](NLP_clustering/trend1.png)

#### 
Clusters 8, 1, and 4 were removed from the trend analysis to provide a clearer view of the remaining clusters' trends. These clusters had significantly higher numbers of patents, which could obscure the trends in clusters with fewer patents.

The trends for the remaining clusters indicate that the number of patents in each cluster has generally increased over time. There is a noticeable synchronous pattern in their growth, suggesting that innovations across these different areas of AI have progressed simultaneously.

- **Cluster 9 (Feature Extraction and Machine Learning)**:
  - **Trend**: This cluster shows the highest number of patents among the remaining clusters. There is a steady increase from around 200 patents in early 2019 to over 500 patents by the end of 2023.
  - **Analysis**: The growth in Cluster 9 suggests a strong and continuous interest in feature extraction and machine learning technologies. This area is foundational to many AI applications, which likely drives the sustained increase in patent filings.

![](NLP_clustering/trend2.png)

## Network

## References
- Breschi, S., & Lissoni, F. (2001). Knowledge Spillovers and Local Innovation Systems: A Critical Survey. *Industrial and Corporate Change*, 10(4), 975-1005.
- Griliches, Z. (1990). Patent statistics as economic indicators: A survey. *Journal of Economic Literature*, 28(4), 1661-1707.
- Henderson, R., Jaffe, A., & Trajtenberg, M. (1998). Universities as a source of commercial technology: A detailed analysis of university patenting, 1965-1988. *Review of Economics and Statistics*, 80(1), 119-127.
- National Academy of Sciences. (2017). *Building America’s Skilled Technical Workforce*. Washington, DC: The National Academies Press.
- Narin, F., Hamilton, K. S., & Olivastro, D. (1997). The increasing linkage between U.S. technology and public science. *Research Policy*, 26(3), 317-330.
- OECD. (2015). *OECD Science, Technology and Industry Scoreboard 2015: Innovation for growth and society*. OECD Publishing.
- Porter, A. L., & Rafols, I. (2009). Is science becoming more interdisciplinary? Measuring and mapping six research fields over time. *Scientometrics*, 81(3), 719-745.
- Singh, J. (2005). Collaborative networks as determinants of knowledge diffusion patterns. *Management Science*, 51(5), 756-770.

## Acknowledgement

We would like to express our sincere gratitude to Professor Jon Clindaniel for his excellent teaching and valuable advice throughout this course. We also extend our thanks to the course TAs, Adam and Wonje, for their timely assistance and support in addressing our questions.
