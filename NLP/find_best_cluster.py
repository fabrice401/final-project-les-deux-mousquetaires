from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, explode, split
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
from pyspark.ml.linalg import Vectors
import matplotlib.pyplot as plt
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

# Determine the optimal number of clusters using the elbow method
cost = []
k_values = range(2, 30)  # Choose a range of cluster numbers from 2 to 30

for k in k_values:
    kmeans = KMeans(k=k, seed=1, featuresCol="features")
    model = kmeans.fit(rescaled_data)
    cost.append(model.summary.trainingCost)  # Get SSE using summary.trainingCost

# Save the results as a CSV file
sse_data = pd.DataFrame({'Number of Clusters': k_values, 'SSE': cost})
sse_data.to_csv('sse_data.csv', index=False)

# Plot the elbow method chart
plt.figure(figsize=(10, 6))
plt.plot(k_values, cost, marker='o')
plt.xlabel('Number of Clusters (k)')
plt.ylabel('Sum of Squared Errors (SSE)')
plt.title('Elbow Method for Optimal k')
# Save the figure to a file
plt.savefig('elbow_plot.png')
