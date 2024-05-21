# Import helper functions
from helper_functions import * 
import mkl
from node2vec import Node2Vec
from pyspark.ml.feature import Word2Vec
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from scipy.spatial.distance import cosine

'''
Data preparation
'''

spark = initialize_spark_session(name='Patent Network Analysis (Assignee Network Overall)')
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


'''
Network Construction
'''
# Create vertices DataFrame with unique assignees
vertices_df = patent_df.select(F.col("assignee").alias("id")).distinct()

# Create edges DataFrame for citation relationships between assignees
edges_df = citing_df.groupBy(
    F.col("citing_assignee").alias("src"), # "source"
    F.col("cited_assignee").alias("dst") # "destination"
).count()

# Create the GraphFrame with vertices and edges
g = GraphFrame(vertices_df, edges_df)

'''
Network Embedding
'''

# Generate the network embedding
node2vec = Node2Vec(graph=g, dimensions=128, walk_length=30, num_walks=200, workers=4)
model = node2vec.fit(window=10, min_count=1, batch_words=4)

# Create a DataFrame with embeddings
embeddings = model.wv.vectors
embedding_df = spark.createDataFrame([(str(i), embeddings[i].tolist()) for i in range(len(embeddings))], ["id", "embedding"])

'''
Patent Vectorization (Word2Vec)
'''
# Train Word2Vec model on abstracts
word2vec = Word2Vec(inputCol="abstract", outputCol="abstract_vector", vectorSize=100, minCount=0)
model = word2vec.fit(patent_df)
abstract_vectors = model.transform(patent_df)

# Combine classification code embeddings and abstract vectors
patent_vectors = abstract_vectors.join(embedding_df, on="id", how="inner")

'''
Calculate Knowledge Exploration Distance
'''
# Define a UDF to calculate cosine distance
cosine_distance_udf = udf(lambda vec1, vec2: float(cosine(vec1, vec2)), FloatType())

# Calculate distances
distances = citing_df.join(patent_vectors.withColumnRenamed("embedding", "citing_embedding"), citing_df.citing_assignee == patent_vectors.id) \
                     .join(patent_vectors.withColumnRenamed("embedding", "cited_embedding"), citing_df.cited_assignee == patent_vectors.id) \
                     .withColumn("distance", cosine_distance_udf("citing_embedding", "cited_embedding"))

# Average knowledge exploration distance
average_distance = distances.agg({"distance": "avg"}).collect()[0][0]
print("Average Knowledge Exploration Distance:", average_distance)

# Top 10 patents with highest knowledge exploration distance
top_distances = distances.orderBy(F.desc("distance")).select("citing_assignee", "distance").show(10)

# Correlation analysis? 