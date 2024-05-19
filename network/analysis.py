from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from graphframes import GraphFrame
from pyspark.graphx import Graph, VertexRDD
import json

'''
Load the Data
'''
# Initialize Spark session
spark = SparkSession.builder \
    .appName("Patent Network Analysis") \
    .getOrCreate()

# Load the patent data
file_path = "../data_cleaning/patent_data_example.csv"
patent_df = spark.read.csv(file_path, header=True, inferSchema=True)


'''
Extract Citation Information
'''
# Define a function to parse the 'citedby' column (to retrieve assigneee info)
def parse_citedby(citedby):
    try:
        citedby_dict = json.loads(citedby.replace("'", "\""))
        return [(citing_id, citing_assignee) for citing_id, citing_assignee in citedby_dict.items()]
    except:
        return []
# Register the function as a UDF
parse_citedby_udf = F.udf(parse_citedby, "array<struct<citing_id:string, citing_assignee:string>>")

# Apply the UDF to extract citing information
parsed_citedby_df = patent_df.withColumn("citing_info", F.explode(parse_citedby_udf(F.col("citedby"))))

# Select relevant columns and extract assignees
citing_df = parsed_citedby_df.select(
    F.col("assignee").alias("cited_assignee"),
    "citing_info.citing_assignee"
)

# Show the extracted information
print("Top five rows of citating dataframe of cited assignees and citing assignees: ")
print(citing_df.show(5))


'''
Create Vertices and Edges for the Assignee Citation Network
'''

# Create vertices DataFrame with unique assignees
vertices_df = patent_df.select(F.col("assignee").alias("id")).distinct()

# Create edges DataFrame for citation relationships between assignees
edges_df = citing_df.groupBy(
    F.col("citing_assignee").alias("src"), # "source"
    F.col("cited_assignee").alias("dst") # "destination"
).count()

# Show vertices and edges
print("Top five rows of vertices_df: ")
vertices_df.show(5)
print()
print("Top five rows of edges_df: ")
edges_df.show(5)

'''
Construct the Assignee Citation Network
'''
# Create the GraphFrame with vertices and edges
g = GraphFrame(vertices_df, edges_df)

'''
Conduct network analysis
'''

