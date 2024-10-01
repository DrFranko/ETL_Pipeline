from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("Kafka ETL").getOrCreate()

# Read data from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_name") \
    .load()

# Select value (message content) from Kafka DataFrame
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Display streaming data
kafka_df.writeStream.format("console").start().awaitTermination()
