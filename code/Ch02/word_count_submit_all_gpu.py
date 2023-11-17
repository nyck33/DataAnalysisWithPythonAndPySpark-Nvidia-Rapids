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
parquet_directory = "/home/nyck33/Documents/DataEngineering/DataAnalysisWithPythonAndPySpark/code/data/gutenberg_books/gutentberg_1342.parquet"

#pdf = spark.read.text(directory)

'''
with open(directory, 'r') as file:
    lines = file.readlines()
pdf = pd.DataFrame(lines, columns=['line'])
'''

#pandas_df = pd.read_parquet(parquet_directory)

#print(pandas_df.head())
# Convert Pandas DataFrame to cuDF DataFrame
#cudf_df = cudf.DataFrame.from_pandas(pandas_df)
# read parquet file into cudf dataframe
cudf_df = cudf.read_parquet(parquet_directory)

# Perform operations on the GPU
cudf_df['value'] = cudf_df['value'].str.lower()
cudf_df = cudf_df['value'].str.split(' ', expand=True).stack().reset_index(drop=True).to_frame(name='word')
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
