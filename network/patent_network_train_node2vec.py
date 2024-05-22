from helper_functions import *
from pyspark.ml.feature import Tokenizer, Word2Vec
from node2vec import Node2Vec
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Initialize Spark session
spark = initialize_spark_session('node2vec_training', memory_size="128g")

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
node2vec_model_path = "trained_embedding_models/node2vec_model"
node2vec_model.save(node2vec_model_path)

# Create a DataFrame with embeddings
network_embeddings = node2vec_model.wv.vectors
network_embedding_df = spark.createDataFrame([(str(node), network_embeddings[i].tolist()) for i, node in enumerate(nx_g.nodes)], ["id", "network_embedding"])

# Save the embedded vectors to a Parquet file
network_embedding_df.write.parquet("data/node_embedding.parquet")

# Stop the Spark session
spark.stop()