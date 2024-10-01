from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("HDFS Extraction").getOrCreate()

# Read CSV file from HDFS
csv_df = spark.read.csv("hdfs://namenode:9000/path/to/csv_file.csv", header=True, inferSchema=True)
csv_df.show()

# Read Parquet file from HDFS
parquet_df = spark.read.parquet("hdfs://namenode:9000/path/to/parquet_file")
parquet_df.show()

# You can return the DataFrames or write them to an intermediary format for further processing
