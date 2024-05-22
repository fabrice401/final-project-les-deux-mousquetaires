from helper_functions import *
from pyspark.sql.types import StringType
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Initialize Spark session
spark = initialize_spark_session('PatentDataPreparation', memory_size="128g")

# Load the patent data
file_path = "../data_cleaning/all_patent_info.csv"
patent_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Convert the 'date' column to DateType
patent_df = patent_df.withColumn("publication date", F.to_date(F.col("publication date"), "yyyy-MM-dd"))

# Add year and quarter columns
patent_df = patent_df.withColumn("year", F.year(F.col("publication date")))
patent_df = patent_df.withColumn("quarter", F.quarter(F.col("publication date")))

# Combine year and quarter into a single column formatted as 'YYYYQX'
patent_df = patent_df.withColumn("year_quarter", F.concat(F.col("year"), F.lit("Q"), F.col("quarter")))

# Load the cluster information data
excel_path = "../NLP_clustering/df_with_clusters_merged.xlsx"
clusters_pd_df = pd.read_excel(excel_path)
clusters_spark_df = spark.createDataFrame(clusters_pd_df)

# Define the cluster number and cluster name lists
cluster_numbers = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
cluster_names = [
    "General Machine Learning and Systems",
    "Image Processing and Recognition",
    "Document Management and Text Processing",
    "Sample Analysis and Training Models",
    "User Interaction and Content Management",
    "Virtual Reality and Augmented Reality",
    "Resource Management and Communication",
    "Authentication and Security",
    "Signal Processing and Wireless Communication",
    "Feature Extraction and Machine Learning",
    "Financial Transactions and Payment Systems",
    "Audio Processing and Speech Recognition"
]

# Create a dictionary mapping cluster numbers to names
cluster_mapping = dict(zip(cluster_numbers, cluster_names))

# Register the UDF using the mapping dictionary
get_cluster_name_udf = F.udf(lambda x: cluster_mapping.get(x), StringType())

# Apply the UDF to create the cluster_name column
clusters_spark_df = clusters_spark_df.withColumn("cluster_name", get_cluster_name_udf(F.col("keywords_cluster")))

patent_df_joined = patent_df.join(clusters_spark_df, "id", "left")
# Select columns of interest
patent_df_joined = patent_df_joined.select('id', 'abstract', 'citedby', 'year_quarter', 'cluster_name')

# Save the joined data
# Repartition the DataFrame
patent_df_joined = repartition_df(patent_df_joined, 200)

# Checkpoint the DataFrame
patent_df_joined = checkpoint_df(patent_df_joined)

# Save the joined data
patent_df_joined.write.parquet("data/patent.parquet")
print("Combined data saved to midway local directory.")

'''
Store vertices and edges of patent citaton network
'''
# Register the function as a UDF
parse_citedby_udf = F.udf(parse_citedby, "array<struct<citing_id:string, citing_assignee:string>>")

# Apply the UDF to extract citing information
parsed_citedby_df = patent_df_joined.withColumn("citing_info", F.explode(parse_citedby_udf(F.col("citedby"))))

# Adjust citing_id to have the same format as cited_id (specifically, with two "-")
# Extract parts using regular expressions
formatted_citing_df = parsed_citedby_df.withColumn(
    "number_part", F.regexp_extract(F.col("citing_info.citing_id"), r"US(\d+)", 1)
)

# Extract the number_suffix by removing the extracted parts from the original string
formatted_citing_df = formatted_citing_df.withColumn(
    "letter_number_suffix", F.expr("substring(citing_info.citing_id, length('US') + length(number_part) + 1, length(citing_info.citing_id))")
)

# Concatenate parts to form the correctly formatted citing_id
formatted_citing_df = formatted_citing_df.withColumn(
    "citing_id", F.concat(
        F.lit("US"),
        F.lit("-"),
        F.col("number_part"),
        F.lit("-"),
        F.col("letter_number_suffix")
    )
)

# Select relevant columns and extract ids
citing_df = formatted_citing_df.select(
    F.col("id").alias("cited_id"),
    F.col("citing_id")
)

# Create the GraphFrame with vertices and edges
vertices_df = patent_df_joined.select(F.col("id")).distinct().dropna()
edges_df = citing_df.groupBy(
    F.col("citing_id").alias("src"),
    F.col("cited_id").alias("dst")
).count().dropna()

# Save data
citing_df.write.parquet("data/citing_df.parquet")
vertices_df.write.parquet("data/vertices.parquet")
edges_df.write.parquet("data/edges.parquet")
print("Vertices and edges saved to midway local directory.")

# Stop the Spark session
spark.stop()