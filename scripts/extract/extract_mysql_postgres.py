from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Define JDBC properties
jdbc_url = "jdbc:mysql://localhost:3306/database_name"
jdbc_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read data from MySQL
mysql_df = spark.read.jdbc(url=jdbc_url, table="table_name", properties=jdbc_properties)

# Show the extracted data
mysql_df.show()

# Similarly, PostgreSQL extraction can be done with
# jdbc_url = "jdbc:postgresql://localhost:5432/database_name"