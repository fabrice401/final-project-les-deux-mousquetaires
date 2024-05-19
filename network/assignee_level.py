from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Patent Network Analysis") \
    .getOrCreate()

# Load the patent data
file_path = "../data_cleaning/patent_data_example.csv"
patent_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Define a function to parse the 'citedby' column (to retrieve assignee info)
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
print("Top five rows of citing dataframe of cited assignees and citing assignees: ")
citing_df.show(5)

# Create vertices DataFrame with unique assignees
vertices_df = patent_df.select(F.col("assignee").alias("id")).distinct()

# Add an index column for numerical community IDs
vertices_df = vertices_df.withColumn("community", F.monotonically_increasing_id())

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

# Create the GraphFrame with vertices and edges
g = GraphFrame(vertices_df, edges_df)

# Calculate degree centrality
degree_df = g.degrees
print("Assignees with top 10 degree centrality: ")
degree_df.orderBy(F.desc("degree")).show(10)

# Calculate in-degree centrality
in_degree_df = g.inDegrees
print("Assignees with top 10 in-degree centrality: ")
in_degree_df.orderBy(F.desc("inDegree")).show(10)

# Calculate out-degree centrality
out_degree_df = g.outDegrees
print("Assignees with top 10 out-degree centrality: ")
out_degree_df.orderBy(F.desc("outDegree")).show(10)

# Calculate PageRank centrality
pagerank_df = g.pageRank(resetProbability=0.15, tol=0.01)
print("Assignees with top 10 PageRank centrality: ")
pagerank_df.vertices.select("id", "pagerank").orderBy(F.desc("pagerank")).show(10)

# Create a new GraphFrame with the updated vertices DataFrame
g = GraphFrame(vertices_df, edges_df)

# Define message function to propagate community ID
msg_func = AM.src["community"]

# Iterative community propagation
for i in range(10):  # Adjust the number of iterations as needed
    # Aggregate messages to find minimum community ID among neighbors
    agg = g.aggregateMessages(F.min(msg_func).alias("new_community"), sendToSrc=msg_func, sendToDst=msg_func)
    # Join new community assignments with vertices
    g = GraphFrame(g.vertices.join(agg, on="id", how="left"), g.edges)
    # Update community column with new assignments
    g = GraphFrame(g.vertices.withColumn("community", F.coalesce(g.vertices["new_community"], g.vertices["community"])), g.edges)
    # Drop temporary column
    g = GraphFrame(g.vertices.drop("new_community"), g.edges)

# Show the resulting communities
print("Communities detected using Louvain method:")
g.vertices.select("id", "community").show(5)