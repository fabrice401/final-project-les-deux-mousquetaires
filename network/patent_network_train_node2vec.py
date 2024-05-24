from helper_functions import *
from pyspark.ml.feature import Tokenizer, Word2Vec
from node2vec import Node2Vec
from gensim.models import KeyedVectors
import os
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Initialize Spark session
spark = initialize_spark_session('node2vec_training')

# Load the prepared data
patent_df = spark.read.parquet("data/patent.parquet")
vertices_df = spark.read.parquet("data/vertices.parquet")
edges_df = spark.read.parquet("data/edges.parquet")

'''
Construct the graph
'''
g = GraphFrame(vertices_df, edges_df)

# Convert GraphFrame to NetworkX graph
nx_g = graphframe_to_networkx_directed(g)

# Generate the network embedding
node2vec = Node2Vec(graph=nx_g, dimensions=128, walk_length=30, num_walks=200, workers=4)
node2vec_model = node2vec.fit(window=10, min_count=1, batch_words=4)

# Save the Node2Vec model
node2vec_model_path = "data/node2vec_model/network_embedding"
# Ensure the directory exists
directory = os.path.dirname(node2vec_model_path)
if not os.path.exists(directory):
    os.makedirs(directory)
node2vec_model.save(node2vec_model_path)

# Stop the Spark session
spark.stop()