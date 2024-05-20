# Import helper functions
from helper_functions import * 

'''
Prepare data for constructing the network 
'''

spark = initialize_spark_session(name='Patent Network Analysis')
# Load the patent data
file_path = "../data_cleaning/patent_data_example.csv"
patent_df = spark.read.csv(file_path, header=True, inferSchema=True)

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
print("Top ten rows of citing dataframe of cited assignees and citing assignees: ")
citing_df.show(10)

# Create vertices DataFrame with unique assignees
vertices_df = patent_df.select(F.col("assignee").alias("id")).distinct()

# Add an index column for numerical community IDs
vertices_df = vertices_df.withColumn("community", F.monotonically_increasing_id())

# Create edges DataFrame for citation relationships between assignees
edges_df = citing_df.groupBy(
    F.col("citing_assignee").alias("src"), # "source"
    F.col("cited_assignee").alias("dst") # "destination"
).count()


'''
Construct the network
'''
# Create the GraphFrame with vertices and edges
g = GraphFrame(vertices_df, edges_df)


'''
Perform graph analysis (centrality measures for each assignee)
'''
# Calculate degree centrality
degree_df, in_degree_df, out_degree_df = calculate_degree_centrality(g)
print("Assignees with top 10 degree centrality: ")
degree_df.orderBy(F.desc("degree")).show(10)
print("Assignees with top 10 in-degree centrality: ")
in_degree_df.orderBy(F.desc("inDegree")).show(10)
print("Assignees with top 10 out-degree centrality: ")
out_degree_df.orderBy(F.desc("outDegree")).show(10)

# Calculate PageRank centrality
pagerank_df = calculate_pagerank_centrality(g)
print("Assignees with top 10 PageRank centrality: ")
pagerank_df.vertices.select("id", "pagerank").orderBy(F.desc("pagerank")).show(10)

# Calculate betweenness and closeness centrality
betweenness_spark_df, closeness_spark_df = calculate_betweenness_closeness_centrality(
    edges_df, spark_session=spark
    )
# Show top nodes by betweenness centrality
betweenness_spark_df.orderBy(F.desc("betweenness")).show(10)
# Show top nodes by closeness centrality
closeness_spark_df.orderBy(F.desc("closeness")).show(10)

'''
Perform graph analysis (aggregated/overall measures)
'''
overall_metrics = calculate_overall_network_metrics(vertices_df, edges_df, 
                                                    degree_df, in_degree_df, 
                                                    out_degree_df, betweenness_spark_df, 
                                                    closeness_spark_df)

'''
Community detection
'''
# Use label propagation community detection algorithms 
cd_result = g.labelPropagation(maxIter=10)
print("Top 10 communities")
cd_result.groupBy("label").count().orderBy(F.desc("count")).show(10)

# Assign communities to vertices
vertices_with_communities = g.vertices.join(cd_result, "id")
# Calculate overall measures for each community
community_metrics_df = calculate_community_metrics(vertices_with_communities, g.edges, spark)
community_metrics_df.show()

# Probably also need to visualize the network
