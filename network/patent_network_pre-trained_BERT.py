from transformers import BertTokenizer, BertModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PreTrainedBERT") \
    .config("spark.executor.memory", "64g") \
    .config("spark.driver.memory", "64g") \
    .config("spark.sql.shuffle.partitions", "400") \
    .getOrCreate()

# Load the prepared data
patent_df = spark.read.parquet("data/patent.parquet")

# Initialize BERT tokenizer and model
model_name = 'bert-base-uncased'

# Download the tokenizer and model
tokenizer = BertTokenizer.from_pretrained(model_name)
model = BertModel.from_pretrained(model_name)

# Define a UDF to compute BERT embeddings
def compute_bert_embedding(text):
    inputs = tokenizer(text, return_tensors='pt', truncation=True, padding=True)
    outputs = model(**inputs)
    return outputs.last_hidden_state.mean(dim=1).detach().numpy().tolist()[0]

# Register the UDF
compute_bert_embedding_udf = udf(compute_bert_embedding, ArrayType(FloatType()))

# Repartition the DataFrame to increase parallelism
patent_df = patent_df.repartition(1000)

# Apply the UDF to the 'abstract' column to compute BERT embeddings
bert_embeddings_df = patent_df.withColumn("abstract_vector", compute_bert_embedding_udf(patent_df["abstract"]))

# Select only the necessary columns to save
bert_embeddings_df = bert_embeddings_df.select("id", "abstract_vector")

# Save the BERT embeddings to a Parquet file with optimal configurations
bert_embeddings_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("data/abstract_vectors.parquet")

# Stop the Spark session
spark.stop()