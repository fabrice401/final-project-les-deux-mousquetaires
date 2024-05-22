from helper_functions import *
from pyspark.ml.feature import Tokenizer, Word2Vec
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Initialize Spark session
spark = initialize_spark_session('word2vec_training', memory_size="512g", num_partitions=400)

# Load the prepared data
patent_df = spark.read.parquet("data/patent.parquet")

'''
Train the word embedding model on patent abstract
'''
# Tokenize the abstract column
tokenizer = Tokenizer(inputCol="abstract", outputCol="words")
words_df = tokenizer.transform(patent_df.filter(patent_df.abstract.isNotNull()))

# Train Word2Vec model on abstracts
word2vec = Word2Vec(inputCol="words", outputCol="abstract_vector", vectorSize=128, minCount=3)
word2vec_model = word2vec.fit(words_df)

# Save the Word2Vec model
word2vec_model_path = "data/trained_word2vec_model"
word2vec_model.save(word2vec_model_path)

# Transform the data to get the embedded vectors
abstract_vectors = word2vec_model.transform(words_df)

# Select only the necessary columns to save
abstract_vectors = abstract_vectors.select("id", "abstract_vector")

# Save the embedded vectors to a Parquet file
abstract_vectors.write.parquet("data/abstract_vectors.parquet")

# Stop the Spark session
spark.stop()
