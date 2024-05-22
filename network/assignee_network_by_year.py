# Import helper functions
from helper_functions import * 

'''
Suppress user warning message in .out file
'''
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

'''
Prepare data for constructing the network 
'''

spark = initialize_spark_session(name='Patent Network Analysis (Assignee Network By Year)')

# Load the patent data
file_path = "../data_cleaning/patent_data_example.csv"
patent_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Register the function as a UDF
parse_citedby_udf = F.udf(parse_citedby, "array<struct<citing_id:string, citing_assignee:string>>")

# Apply the UDF to extract citing information
parsed_citedby_df = patent_df.withColumn("citing_info", F.explode(parse_citedby_udf(F.col("citedby"))))

# Convert publication date to date type and extract year
parsed_citedby_df = parsed_citedby_df.withColumn("publication_date", F.to_date(F.col("publication date"), "yyyy-MM-dd"))
parsed_citedby_df = parsed_citedby_df.withColumn("year", F.year("publication_date"))

# Select relevant columns and extract assignees
citing_df = parsed_citedby_df.select(
    F.col("assignee").alias("cited_assignee"),
    "citing_info.citing_assignee",
    "year"
)

# Create vertices DataFrame with unique assignees
vertices_df = patent_df.select(F.col("assignee").alias("id")).distinct()

# Add an index column for numerical community IDs
vertices_df = vertices_df.withColumn("community", F.monotonically_increasing_id())

# Collect all years from the data
years = citing_df.select("year").distinct().rdd.flatMap(lambda x: x).collect()

'''
Perform analysis for each year and collect the results
'''
yearly_metrics = []
for year in years:
    metrics = perform_yearly_analysis(year, citing_df, vertices_df, spark)
    if metrics:
        yearly_metrics.append(metrics)

# Convert the results to a Spark DataFrame
yearly_metrics_df = spark.createDataFrame(yearly_metrics)

# Show the yearly metrics
yearly_metrics_df.show()

# Save the results to a CSV file
# output_path = "/mnt/data/yearly_metrics.csv"
# yearly_metrics_df.write.csv(output_path, header=True)

# print(f"Yearly metrics saved to {output_path}")

'''
Terminate the spark session 
'''
spark.stop()