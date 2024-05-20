import json
import networkx as nx
from pyspark.sql import SparkSession
import pandas as pd

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
    G = nx.from_pandas_edgelist(edges_pd, source='src', target='dst', create_using=nx.DiGraph())
    
    betweenness_dict = nx.betweenness_centrality(G, normalized=True)
    betweenness_df = pd.DataFrame(list(betweenness_dict.items()), columns=['id', 'betweenness'])
    betweenness_spark_df = spark_session.createDataFrame(betweenness_df)

    closeness_dict = nx.closeness_centrality(G)
    closeness_df = pd.DataFrame(list(closeness_dict.items()), columns=['id', 'closeness'])
    closeness_spark_df = spark_session.createDataFrame(closeness_df)
    
    return betweenness_spark_df, closeness_spark_df