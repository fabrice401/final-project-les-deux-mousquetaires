import pyspark.sql.functions as F
from graphframes import GraphFrame
import json
import networkx as nx
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
def initialize_spark_session(name):
    spark = SparkSession.builder \
        .appName(name) \
        .getOrCreate()
    
    return spark

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

def calculate_overall_network_metrics(vertices_df, edges_df, degree_df, 
                                      in_degree_df, out_degree_df, 
                                      betweenness_spark_df, closeness_spark_df):
    # 1. Network Density
    num_nodes = vertices_df.count()
    possible_edges = num_nodes * (num_nodes - 1)
    num_actual_edges = edges_df.count()
    network_density = float(num_actual_edges) / possible_edges if possible_edges != 0 else 0.0
    print(f"Network Density: {network_density}")

    # 2. Average degree
    avg_degree = degree_df.select(F.avg("degree").cast("double")).first()[0]
    print(f"Network Average Degree: {avg_degree}")

    # 3. Average in-degree
    avg_in_degree = in_degree_df.select(F.avg("inDegree").cast("double")).first()[0]
    print(f"Network Average In-Degree: {avg_in_degree}")

    # 4. Average out-degree
    avg_out_degree = out_degree_df.select(F.avg("outDegree").cast("double")).first()[0]
    print(f"Network Average Out-Degree: {avg_out_degree}")

    # 5. Average betweenness
    if betweenness_spark_df.count() > 0:
        avg_betweenness = betweenness_spark_df.select(F.avg("betweenness").cast("double")).first()[0]
    else:
        avg_betweenness = 0.0
    print(f"Network Average Betweenness: {avg_betweenness}")

    # 6. Average closeness
    if closeness_spark_df.count() > 0:
        avg_closeness = closeness_spark_df.select(F.avg("closeness").cast("double")).first()[0]
    else:
        avg_closeness = 0.0
    print(f"Network Average Closeness: {avg_closeness}")

    return {
        "network_density": network_density,
        "avg_degree": avg_degree,
        "avg_in_degree": avg_in_degree,
        "avg_out_degree": avg_out_degree,
        "avg_betweenness": avg_betweenness,
        "avg_closeness": avg_closeness
    }

# Filter edges for a community
def filter_edges_for_community(edges_df, community_vertices):
    community_vertices_set = set(community_vertices)
    return edges_df.filter(F.col("src").isin(community_vertices_set) & F.col("dst").isin(community_vertices_set))

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