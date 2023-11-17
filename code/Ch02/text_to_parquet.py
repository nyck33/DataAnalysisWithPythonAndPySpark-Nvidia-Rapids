from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Text to Parquet").getOrCreate()

directory = "/home/nyck33/Documents/DataEngineering/DataAnalysisWithPythonAndPySpark/code/data/gutenberg_books/*.txt"

# Read text files into a Spark DataFrame
text_df = spark.read.text(directory)

# Write the DataFrame to Parquet format
text_df.write.mode("overwrite").parquet("/home/nyck33/Documents/DataEngineering/DataAnalysisWithPythonAndPySpark/code/data/gutenberg_books/gutentberg_1342.parquet")
