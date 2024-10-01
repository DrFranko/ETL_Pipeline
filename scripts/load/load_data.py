from pyspark.sql import SparkSession
import logging
from prometheus_client import start_http_server, Counter

# Setup logging configuration
logging.basicConfig(
    filename='etl_pipeline.log',  # Log file path
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize Prometheus metrics
successful_loads = Counter('successful_loads', 'Total number of successful load operations')
failed_loads = Counter('failed_loads', 'Total number of failed load operations')

# Start the Prometheus metrics server
start_http_server(8000)  # Expose metrics on port 8000

# Initialize Spark session
spark = SparkSession.builder.appName("ETL Loading").getOrCreate()

# Load data to MySQL/PostgreSQL, HDFS, and Delta Lake
try:
    # Write data back to MySQL/PostgreSQL
    agg_df.write.jdbc(url=jdbc_url, table="output_table", mode="overwrite", properties=jdbc_properties)
    logging.info("Data loaded successfully to MySQL/PostgreSQL.")
    successful_loads.inc()  # Increment success counter

    # Save DataFrame as Parquet in HDFS
    agg_df.write.parquet("hdfs://namenode:9000/path/to/output.parquet", mode="overwrite")
    logging.info("Data saved successfully as Parquet in HDFS.")
    successful_loads.inc()  # Increment success counter

    # Save DataFrame to Delta Lake
    agg_df.write.format("delta").mode("overwrite").save("/path/to/delta-table")
    logging.info("Data saved successfully to Delta Lake.")
    successful_loads.inc()  # Increment success counter

except Exception as e:
    logging.error(f"Error during loading data: {e}")
    failed_loads.inc()  # Increment failure counter

finally:
    spark.stop()  # Stop the Spark session