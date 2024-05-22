from helper_functions import *
import warnings
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import FloatType
warnings.filterwarnings("ignore", category=UserWarning)

# Initialize Spark session
spark = initialize_spark_session('calculate_ked', memory_size="128g")

'''
Read prepared data
'''
abstract_vectors = spark.read.parquet("data/abstract_vectors.parquet")
network_embedding_df = spark.read.parquet("data/node_embedding.parquet")
citing_df = spark.read.parquet("data/citing_df.parquet")

'''
Concatenate embeddings
'''
# Combine classification code embeddings and abstract vectors
patent_vectors = abstract_vectors.dropna(subset=["abstract_vector"]).join(network_embedding_df.dropna(), on="id", how="inner")
combine_vectors_udf = F.udf(combine_vectors, VectorUDT())

# Apply the UDF to create a new column with the combined vectors
patent_vectors = patent_vectors.withColumn("combined_vector", combine_vectors_udf(F.col("abstract_vector"), F.col("network_embedding")))

'''
Calculate Knowledge Exploration Distance
'''
# Define UDFs for cosine similarity and KED
cosine_similarity_udf = F.udf(cosine_similarity, FloatType())
ked_udf = F.udf(ked, FloatType())

# Alias the DataFrames to remove ambiguity
patent_vectors_alias_1 = patent_vectors.alias("pv1")
patent_vectors_alias_2 = patent_vectors.alias("pv2")

# Join the DataFrames using the aliases
joined_df = citing_df.join(patent_vectors_alias_1.withColumnRenamed("combined_vector", "citing_vector"), citing_df.citing_id == patent_vectors_alias_1.id) \
                     .join(patent_vectors_alias_2.withColumnRenamed("combined_vector", "cited_vector"), citing_df.cited_id == patent_vectors_alias_2.id)

# Group by citing_id and aggregate cited_vectors
grouped_df = joined_df.groupBy("citing_id", "citing_vector").agg(F.collect_list("cited_vector").alias("cited_vectors"))

# Calculate KED for each citing_id
ked_df = grouped_df.withColumn("ked", ked_udf(F.col("citing_vector"), F.col("cited_vectors")))

# # Add year_quarter and cluster level to the KED DataFrame
# ked_df = ked_df.join(patent_df.select("id", "year_quarter", "cluster_name"), ked_df.citing_id == patent_df.id)

# # Group by year_quarter and calculate the average KED for each group
# average_ked_by_group = ked_df.groupBy("year_quarter").agg(F.avg("ked").alias("average_ked"))

# # Group by cluster and calculate the average KED for each group
# average_ked_by_group = ked_df.groupBy("cluster_name").agg(F.avg("ked").alias("average_ked"))

# Calculate the average KED
average_ked = ked_df.agg({"ked": "avg"}).collect()[0][0]
print("Average Knowledge Exploration Distance:", average_ked)

# Show the top 10 patents with the highest KED
top_ked = ked_df.orderBy(F.col("ked").desc()).select("citing_id", "ked").show(10)

# Correlation analysis? 
'''
Terminate the spark session 
'''
spark.stop()