import requests
import pandas as pd
from pyspark.sql import SparkSession

# Fetch data from REST API
response = requests.get('https://api.example.com/data')
data = response.json()

# Convert to Pandas DataFrame
df = pd.DataFrame(data)

# Initialize Spark session
spark = SparkSession.builder.appName("API ETL").getOrCreate()

# Convert Pandas DataFrame to Spark DataFrame
api_df = spark.createDataFrame(df)

# Show API data
api_df.show()
