from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, explode, quarter, year, to_date
import pyspark.sql.functions as F
import pandas as pd

# Create Spark Session
spark = SparkSession.builder.appName("PatentNLP").getOrCreate()

# Load data from S3
df = spark.read.csv('all_patent_info.csv', header=True, inferSchema=True)

# Select abstract
abstract = df.select("abstract").filter(col("abstract").isNotNull())

# Text preprocessing
# Tokenization
tokenizer = Tokenizer(inputCol="abstract", outputCol="words")
words_data = tokenizer.transform(abstract)

# Remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
filtered_data = remover.transform(words_data)

# Compute TF-IDF
cv = CountVectorizer(inputCol="filtered", outputCol="raw_features", vocabSize=10000, minDF=2.0)
cv_model = cv.fit(filtered_data)
featurized_data = cv_model.transform(filtered_data)

idf = IDF(inputCol="raw_features", outputCol="features")
idf_model = idf.fit(featurized_data)
rescaled_data = idf_model.transform(featurized_data)

cluster_num = 11
# Clustering
kmeans = KMeans(k=cluster_num, seed=1, featuresCol="features")
model = kmeans.fit(rescaled_data)

# Show results
clusters = model.transform(rescaled_data)
clusters.show()

# Get the clustering results
clusters = model.transform(rescaled_data)

# Compute word frequency for each cluster
def get_top_words(cluster_df, n=50):
    # Explode the words in the filtered column
    words = cluster_df.withColumn("word", explode("filtered"))
    
    # Compute the frequency of each word
    word_counts = words.groupBy("word").count()
    
    # Sort by frequency and take the top n words
    top_words = word_counts.orderBy(col("count").desc()).limit(n)
    
    return top_words

# Analyze each cluster
for i in range(cluster_num):
    cluster_df = clusters.filter(col("prediction") == i)
    top_words = get_top_words(cluster_df)
    
    print(f"Cluster {i} top words:")
    top_words_df = top_words.toPandas()  # Convert to Pandas DataFrame
    print(top_words_df.head(50))  # Display the top 50 rows

# Add the prediction column from the clustering results to the original DataFrame
df_with_clusters = df.join(clusters.select("abstract", "prediction"), on="abstract", how="left")

# Rename the prediction column to keywords_cluster
df_with_clusters = df_with_clusters.withColumnRenamed("prediction", "keywords_cluster")

# Select the required columns
df_to_save = df_with_clusters.select("id", "publication date", "keywords_cluster")

# Save as CSV file
df_to_save_pd = df_to_save.toPandas()
df_to_save_pd.to_excel("df_with_clusters.xlsx", index=False)
