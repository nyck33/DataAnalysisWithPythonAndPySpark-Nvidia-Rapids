from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import cudf
import pandas as pd
import os

# Initialize SparkSession with the RAPIDS plugin
rapids_jar_path = os.path.expanduser("~/Documents/DataEngineering/rapids-4-spark_2.12-23.10.0.jar")

spark = SparkSession.builder \
    .appName("GPU Accelerated Word Count") \
    .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
    .config("spark.rapids.memory.gpu.pooling.enabled", "false") \
    .config("spark.jars", rapids_jar_path) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read the text file into a Pandas DataFrame
directory = "/home/nyck33/Documents/DataEngineering/DataAnalysisWithPythonAndPySpark/code/data/gutenberg_books/1342-0.txt"

with open(directory, 'r') as file:
    lines = file.readlines()
pdf = pd.DataFrame(lines, columns=['line'])

# Convert Pandas DataFrame to cuDF DataFrame
cudf_df = cudf.DataFrame.from_pandas(pdf)

# Perform operations on the GPU
cudf_df['line'] = cudf_df['line'].str.lower()
cudf_df = cudf_df['line'].str.split(' ', expand=True).stack().reset_index(drop=True).to_frame(name='word')
cudf_df['word'] = cudf_df['word'].str.extract('([a-z\']*)')
cudf_df = cudf_df[cudf_df['word'] != '']

# Count word occurrences
word_counts = cudf_df.groupby('word').size().reset_index(name='count')

# Sort the results on the GPU
word_counts = word_counts.sort_values(by='count', ascending=False)

# Define the schema for the Spark DataFrame
schema = StructType([
    StructField("word", StringType(), True),
    StructField("count", LongType(), True)
])

# Convert cuDF DataFrame back to Spark DataFrame with schema
word_counts_spark = spark.createDataFrame(word_counts.to_pandas(), schema=schema)

# Show and save results
word_counts_spark.show(10)
word_counts_spark.coalesce(1).write.mode("overwrite").option("header", "true").csv("output")
