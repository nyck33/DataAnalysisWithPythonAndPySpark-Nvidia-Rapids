### Program Documentation: Workflow Summary

**Objective:**
The purpose of this program is to perform an efficient word count on a collection of text files. To achieve this, we employed both Apache Spark and GPU acceleration via RAPIDS cuDF to handle large datasets and to leverage the processing power of GPUs.

**Workflow Steps:**

1. **Conversion to Parquet:**
   - **Why**: We converted original text files to the Parquet format to benefit from its efficient columnar storage, which improves read and write operations and offers better compression than row-based text files.
   - **How**: Using Apache Spark, we read the text files and wrote them out as Parquet files. This step also implicitly partitions the data, resulting in multiple part files, which is beneficial for distributed processing.


2. **Reading Parquet Files:**
   - **Why**: Reading Parquet files directly into a cuDF DataFrame allows for handling the data in a structured format while leveraging the GPU for performance enhancements inherent in Parquet.
   - **How**: We used cuDF's direct I/O capabilities to read the Parquet files into a cuDF DataFrame, avoiding the need to use Pandas and minimizing memory overhead.

3. **GPU-Accelerated Data Processing:**
   - **Why**: The aim is to utilize the parallel computation capabilities of the GPU to accelerate data processing tasks, which are computationally intensive.
   - **How**: Since the data is already in a cuDF DataFrame after reading from the Parquet file, we proceed directly with GPU-accelerated operations for text processing.

4. **Text Processing with GPU:**
   - **Why**: String operations can be computationally intensive, and performing these on the GPU offers significant performance improvements.
   - **How**: With the data in a cuDF DataFrame, we performed operations such as string lowering, splitting into words, regex extraction, and filtering directly on the GPU.

5. **Word Counting and Sorting:**
   - **Why**: The goal was to count the occurrences of each unique word and sort the results to identify the most frequent words.
   - **How**: We used cuDF's groupby and sort functionalities to count word occurrences and sort them by frequency, utilizing the GPU for fast computation.

6. **Conversion Back to Spark DataFrame:**
   - **Why**: To display the final processed results using Spark and to write out the data, leveraging Spark's distributed writing capabilities.
   - **How**: We converted the cuDF DataFrame back to a Pandas DataFrame and then created a Spark DataFrame from it.

7. **Results Display and Output:**
   - **Why**: To review the top word counts and to persist the results for future use.
   - **How**: We displayed the top results using Spark's `show` method and wrote the entire result set to a CSV file.

**Outcome:**
The program successfully leverages a hybrid approach combining Spark's distributed data processing capabilities with the powerful parallel computation of GPUs via RAPIDS cuDF. This approach is particularly well-suited for large datasets that require intensive text processing and where performance is a critical concern.


### ops on CPU vs. GPU

| Operation                                      | Processing Unit |
|------------------------------------------------|-----------------|
| Initialize SparkSession with RAPIDS plugin     | CPU             |
| Read Parquet directly into cuDF DataFrame      | GPU             |
| Lowercase string operation (`str.lower`)       | GPU             |
| Split string into words (`str.split`)          | GPU             |
| Regular expression extraction (`str.extract`)  | GPU             |
| Filter non-empty words                         | GPU             |
| Group by word and count occurrences            | GPU             |
| Sort word counts (`sort_values`)               | GPU             |
| Convert cuDF DataFrame to Pandas DataFrame (if needed for further processing or output) | CPU             |
| Create Spark DataFrame from Pandas DataFrame (if needed for further processing or output)   | CPU             |
| Display top 10 word counts (`show`)            | CPU             |
| Write Spark DataFrame to CSV file (if required) | CPU             |

Writing the results to a CSV file is a Spark action that typically takes place on the CPU, as it involves I/O operations that are not accelerated by the GPU. The warnings in the output indicate that while the RAPIDS Accelerator is enabled, certain Spark operations related to data writing do not have GPU support and are executed on the CPU.