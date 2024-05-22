import pyspark.sql.functions as F
from graphframes import GraphFrame
import json
import networkx as nx
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

# Initialize Spark session
def initialize_spark_session(name, memory_size="64g", set_local_directory=False, num_partitions=200, checkpoint_dir="spark_checkpoints"):
    # Stop the existing Spark session if any
    existing_spark = SparkSession.builder.getOrCreate()
    if existing_spark:
        existing_spark.stop()

    builder = SparkSession.builder.appName(name)
    
    if set_local_directory:
        builder = builder.config("spark.local.dir", "spark_temp_dir")
    
    if memory_size:
        builder = builder.config("spark.executor.memory", memory_size)
        builder = builder.config("spark.driver.memory", memory_size)
        builder = builder.config("spark.memory.fraction", "0.8")  # Set the fraction of heap space used for execution and storage
        builder = builder.config("spark.memory.storageFraction", "0.2")  # Set the fraction of (spark.memory.fraction) used for storage
        builder = builder.config("spark.executor.heartbeatInterval", "20s")
        builder = builder.config("spark.network.timeout", "300s")
        builder = builder.config("spark.storage.memoryFraction", "0.4")

    # Set the number of partitions
    builder = builder.config("spark.sql.shuffle.partitions", num_partitions)
    
    # Enable checkpointing
    spark = builder.getOrCreate()
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    return spark

def repartition_df(df, num_partitions):
    return df.repartition(num_partitions)

def checkpoint_df(df):
    df = df.checkpoint()
    return df

# Define a function to parse the 'citedby' column (to retrieve assignee info)
def parse_citedby(citedby):
    try:
        citedby_dict = json.loads(citedby.replace("'", "\""))
        return [(citing_id, citing_assignee) for citing_id, citing_assignee in citedby_dict.items()]
    except:
        return []

# Calculates degree centrality for each node from a GraphFrame object (return spark DataFrame)
def calculate_degree_centrality(g):
    degree_df = g.degrees
    in_degree_df = g.inDegrees
    out_degree_df = g.outDegrees
    
    return degree_df, in_degree_df, out_degree_df

# Calculates pagerank centrality for each node from a GraphFrame object (return spark DataFrame)
def calculate_pagerank_centrality(g):
    pagerank_df = g.pageRank(resetProbability=0.15, tol=0.01)
    return pagerank_df

# Calculates betweenness and closeness centrality for each node from a GraphFrame object (return spark DataFrame)
def calculate_betweenness_closeness_centrality(edges_df, spark_session):
    edges_pd = edges_df.select("src", "dst").toPandas()
    if edges_pd.empty:
        betweenness_spark_df = spark_session.createDataFrame([], schema="id STRING, betweenness DOUBLE")
        closeness_spark_df = spark_session.createDataFrame([], schema="id STRING, closeness DOUBLE")
        return betweenness_spark_df, closeness_spark_df
    
    G = nx.from_pandas_edgelist(edges_pd, source='src', target='dst', create_using=nx.DiGraph())
    
    betweenness_dict = nx.betweenness_centrality(G, normalized=True)
    betweenness_df = pd.DataFrame(list(betweenness_dict.items()), columns=['id', 'betweenness'])
    betweenness_spark_df = spark_session.createDataFrame(betweenness_df)

    closeness_dict = nx.closeness_centrality(G)
    closeness_df = pd.DataFrame(list(closeness_dict.items()), columns=['id', 'closeness'])
    closeness_spark_df = spark_session.createDataFrame(closeness_df)
    
    return betweenness_spark_df, closeness_spark_df

# Calculate overall network metrics of the network ]
def calculate_overall_network_metrics(vertices_df, edges_df, degree_df, 
                                      in_degree_df, out_degree_df, pagerank_df,
                                      betweenness_spark_df, closeness_spark_df):
    # 1. Network Density
    num_nodes = vertices_df.count()
    possible_edges = num_nodes * (num_nodes - 1)
    num_actual_edges = edges_df.count()
    network_density = float(num_actual_edges) / possible_edges if possible_edges != 0 else 0.0

    # 2. Average degree
    avg_degree = degree_df.select(F.avg("degree").cast("double")).first()[0]

    # 3. Average in-degree
    avg_in_degree = in_degree_df.select(F.avg("inDegree").cast("double")).first()[0]

    # 4. Average out-degree
    avg_out_degree = out_degree_df.select(F.avg("outDegree").cast("double")).first()[0]

    # 4. Average pagerank
    avg_pagerank = pagerank_df.select(F.avg("pagerank").cast("double")).first()[0]

    # 6. Average betweenness
    if betweenness_spark_df.count() > 0:
        avg_betweenness = betweenness_spark_df.select(F.avg("betweenness").cast("double")).first()[0]
    else:
        avg_betweenness = 0.0

    # 7. Average closeness
    if closeness_spark_df.count() > 0:
        avg_closeness = closeness_spark_df.select(F.avg("closeness").cast("double")).first()[0]
    else:
        avg_closeness = 0.0

    return {
        "network_density": network_density,
        "avg_degree": avg_degree,
        "avg_in_degree": avg_in_degree,
        "avg_out_degree": avg_out_degree,
        "avg_pagerank": avg_pagerank,
        "avg_betweenness": avg_betweenness,
        "avg_closeness": avg_closeness
    }

# Filter edges for a community
def filter_edges_for_community(edges_df, community_vertices):
    community_vertices_set = set(community_vertices)
    return edges_df.filter(F.col("src").isin(community_vertices_set) & F.col("dst").isin(community_vertices_set))

# Calculate network metrices for specific communities
def calculate_community_metrics(vertices_with_communities, edges_df, spark_session):
    community_labels = vertices_with_communities.select("label").distinct().collect()
    community_labels = [row["label"] for row in community_labels]

    metrics_list = []

    for label in community_labels:
        # Filter vertices and edges for the current community
        community_vertices_df = vertices_with_communities.filter(F.col("label") == label)
        community_vertices = community_vertices_df.select("id").rdd.flatMap(lambda x: x).collect()
        community_edges_df = filter_edges_for_community(edges_df, community_vertices)
        
        # Check if the community has edges
        if community_edges_df.count() == 0:
            continue
        
        # Calculate degrees, in-degrees, and out-degrees for the community
        community_graph = GraphFrame(community_vertices_df, community_edges_df)
        community_degree_df, community_in_degree_df, community_out_degree_df = calculate_degree_centrality(community_graph)
        
        # Calculate betweenness and closeness centrality for the community
        community_betweenness_df, community_closeness_df = calculate_betweenness_closeness_centrality(community_edges_df, spark_session)
        
        # Calculate metrics
        metrics = calculate_overall_network_metrics(
            community_vertices_df,
            community_edges_df,
            community_degree_df,
            community_in_degree_df,
            community_out_degree_df,
            community_betweenness_df,
            community_closeness_df
        )
        
        metrics['community_label'] = label
        metrics_list.append(metrics)

    # Convert list of metrics to a Spark DataFrame
    metrics_df = spark_session.createDataFrame(metrics_list)
    
    return metrics_df

# Perform yearly analysis on the patent citation network to derive network measures
def perform_yearly_analysis(year, citing_df, vertices_df, spark_session):
    yearly_edges_df = citing_df.filter(F.col("year") == year).groupBy(
        F.col("citing_assignee").alias("src"), # "source"
        F.col("cited_assignee").alias("dst") # "destination"
    ).count()
    
    if yearly_edges_df.count() == 0:
        return None
    
    # Create the GraphFrame with vertices and edges
    g = GraphFrame(vertices_df, yearly_edges_df)
    
    # Calculate degree centrality
    degree_df, in_degree_df, out_degree_df = calculate_degree_centrality(g)
    
    # Calculate PageRank centrality
    pagerank_df = calculate_pagerank_centrality(g)
    
    # Calculate betweenness and closeness centrality
    betweenness_spark_df, closeness_spark_df = calculate_betweenness_closeness_centrality(
        yearly_edges_df, spark_session=spark_session
    )
    
    # Calculate overall measures
    overall_metrics = calculate_overall_network_metrics(vertices_df, yearly_edges_df, 
                                                        degree_df, in_degree_df, pagerank_df,
                                                        out_degree_df, betweenness_spark_df, 
                                                        closeness_spark_df)
    overall_metrics["year"] = year
    return overall_metrics

# Convert graphframe to networkx
def graphframe_to_networkx_directed(gf):
    nx_graph = nx.DiGraph()
    for row in gf.vertices.collect():
        nx_graph.add_node(row['id'])
    for row in gf.edges.collect():
        nx_graph.add_edge(row['src'], row['dst'], weight=row['count'])
    return nx_graph

# Combine structural and semantic embeddings
def combine_vectors(abstract_vector, network_embedding):
    # Convert lists to DenseVector if needed
    if isinstance(abstract_vector, list):
        abstract_vector = Vectors.dense(abstract_vector)
    if isinstance(network_embedding, list):
        network_embedding = Vectors.dense(network_embedding)
    return Vectors.dense(abstract_vector.toArray().tolist() + network_embedding.toArray().tolist())

# Calculate cosine distance between two vectors
def cosine_similarity(vec1, vec2):
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

# Calculate knowledge exploration distance
def ked(citing_vector, cited_vectors):
    if cited_vectors is None or len(cited_vectors) == 0:
        return None
    distances = [1 - cosine_similarity(citing_vector, vec) for vec in cited_vectors]
    return np.mean(distances)