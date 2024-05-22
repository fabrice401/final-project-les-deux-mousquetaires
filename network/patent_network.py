# Import helper functions
from helper_functions import * 
from node2vec import Node2Vec
from pyspark.ml.feature import Tokenizer, Word2Vec
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import FloatType

'''
Suppress user warning message in .out file
'''
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

'''
Data preparation
'''

spark = initialize_spark_session(name='Patent Network Analysis (Patent Network)',
                                 memory_size='4g',
                                 set_local_directory=True)
# Load the patent data
file_path = "../data_cleaning/patent_data.csv"
patent_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Convert the 'date' column to DateType
patent_df = patent_df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

# Add year and quarter columns
patent_df = patent_df.withColumn("year", F.year(F.col("date")))
patent_df = patent_df.withColumn("quarter", F.quarter(F.col("date")))

# Combine year and quarter into a single column formatted as 'YYYYQX'
patent_df = patent_df.withColumn("year_quarter", F.concat(F.col("year"), F.lit("Q"), F.col("quarter")))

# Register the function as a UDF
parse_citedby_udf = F.udf(parse_citedby, "array<struct<citing_id:string, citing_assignee:string>>")

# Apply the UDF to extract citing information
parsed_citedby_df = patent_df.withColumn("citing_info", F.explode(parse_citedby_udf(F.col("citedby"))))

# Adjust citing_id to have the same format as cited_id (specifically, with two "-")
# Extract parts using regular expressions
formatted_citing_df = parsed_citedby_df.withColumn(
    "number_part", F.regexp_extract(F.col("citing_info.citing_id"), r"US(\d+)", 1)
)

# Extract the number_suffix by removing the extracted parts from the original string
formatted_citing_df = formatted_citing_df.withColumn(
    "letter_number_suffix", F.expr("substring(citing_info.citing_id, length('US') + length(number_part) + 1, length(citing_info.citing_id))")
)

# Concatenate parts to form the correctly formatted citing_id
formatted_citing_df = formatted_citing_df.withColumn(
    "citing_id", F.concat(
        F.lit("US"),
        F.lit("-"),
        F.col("number_part"),
        F.lit("-"),
        F.col("letter_number_suffix")
    )
)

# Select relevant columns and extract ids
citing_df = formatted_citing_df.select(
    F.col("id").alias("cited_id"),
    F.col("citing_id")
)

'''
Network Construction
'''
# Create vertices DataFrame with unique assignees and remove NA values
vertices_df = patent_df.select(F.col("id")).distinct().dropna()

# Create edges DataFrame for citation relationships between assignees
edges_df = citing_df.groupBy(
    F.col("citing_id").alias("src"),
    F.col("cited_id").alias("dst")
).count()

# Remove NA values from edges DataFrame as well
edges_df = edges_df.dropna()

# Create the GraphFrame with vertices and edges
g = GraphFrame(vertices_df, edges_df)

'''
Network Embedding
'''
# Convert GraphFrame to NetworkX graph
nx_g = graphframe_to_networkx_directed(g)

# Generate the network embedding
node2vec = Node2Vec(graph=nx_g, dimensions=128, walk_length=30, num_walks=200, workers=4)
model = node2vec.fit(window=10, min_count=1, batch_words=4)

# Create a DataFrame with embeddings
network_embeddings = model.wv.vectors
network_embedding_df = spark.createDataFrame([(str(node), network_embeddings[i].tolist()) for i, node in enumerate(nx_g.nodes)], ["id", "network_embedding"])

'''
Patent Vectorization (Word2Vec)
'''
# Tokenize the abstract column
tokenizer = Tokenizer(inputCol="abstract", outputCol="words")
words_df = tokenizer.transform(patent_df.filter(patent_df.abstract.isNotNull()))
# Change it later!!!
# words_df = words_df.select("id", "words", "classification", "assignee")
words_df = words_df.select("id", "words", "classification")

# Train Word2Vec model on abstracts
word2vec = Word2Vec(inputCol="words", outputCol="abstract_vector", vectorSize=128, minCount=0)
model = word2vec.fit(words_df)

abstract_vectors = model.transform(words_df)

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
# ked_df = ked_df.join(patent_df.select("id", "year_quarter"), ked_df.citing_id == patent_df.id)

# # Group by year_quarter and cluster level and calculate the average KED for each group
# average_ked_by_group = ked_df.groupBy("year_quarter").agg(F.avg("ked").alias("average_ked"))

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