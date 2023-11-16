# end-of-chapter.py############################################################
#
# Use this to get a free pass from Chapter 2 to Chapter 3.
#
# Remember, with great power comes great responsibility. Make sure you
# understand the code before running it! If necessary, refer to the text in
# Chapter 2.
#
###############################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower, regexp_extract
#from ....bookdir import book_dir

book_dir ="/home/nyck33/Documents/DataEngineering/DataAnalysisWithPythonAndPySpark"

print(f'book_dir: {book_dir}')
spark = SparkSession.builder.getOrCreate()
directory = book_dir + "/code/Ch02/end_of_chapter.py"

book = spark.read.text(directory)

lines = book.select(split(book.value, " ").alias("line"))

words = lines.select(explode(col("line")).alias("word"))

words_lower = words.select(lower(col("word")).alias("word_lower"))

words_clean = words_lower.select(
    regexp_extract(col("word_lower"), "[a-z]*", 0).alias("word")
)

words_nonull = words_clean.where(col("word") != "")

print("Number of words:", words_nonull.count())
