from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, regexp_extract, split, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import cudf
import cupy as cp
import pyspark.sql.functions as F
import pandas as pd


from pyspark.sql import SparkSession

# Initialize SparkSession with the RAPIDS plugin
spark = SparkSession.builder \
    .appName("Counting word occurrences from a book with GPU") \
    .config("spark.plugins", "com.nvidia.spark.SQLPlugin") \
    .config("spark.rapids.memory.gpu.pooling.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Reading the text file and performing transformations using operations compatible with the GPU
df = (spark.read.text("../../data/gutenberg_books/1342-0.txt")
      .selectExpr("split(value, ' ') as line")  # Use `selectExpr` for compatible split operation
      .withColumn("word", F.explode(F.col("line")))
      .withColumn("word", F.lower(F.col("word")))
      .withColumn("word", F.regexp_extract(F.col("word"), "[a-z']*", 0))
      .filter(F.col("word") != ""))

# The remaining code for conversion to cuDF and word counting can remain the same
# ... (rest of your code)

# Convert Spark DataFrame to cuDF DataFrame
cudf_df = cudf.DataFrame.from_pandas(df.toPandas())

# Define a Pandas UDF to count the word occurrences
@pandas_udf("word string, count int", PandasUDFType.GROUPED_MAP)
def count_words_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    # Count the word occurrences using cuDF
    gdf = cudf.from_pandas(pdf)
    counts = gdf.groupby("word").count().reset_index()
    # Convert cuDF DataFrame to Pandas DataFrame
    pdf_counts = counts.to_pandas()
    return pdf_counts

# Apply the Pandas UDF to the cuDF DataFrame
results = cudf_df.groupby("word").apply(count_words_udf)

# Convert Pandas DataFrame to Spark DataFrame
schema = StructType(
    [
        StructField("word", StringType(), True),
        StructField("count", IntegerType(), True),
    ]
)
results_spark = spark.createDataFrame(results.to_pandas(), schema=schema)

results_spark.orderBy("count", ascending=False).show(10)
results_spark.coalesce(1).write.mode("overwrite").option("header", "true").csv("output")
