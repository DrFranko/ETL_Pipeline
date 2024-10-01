from pyspark.sql.functions import col, sum, avg

# Data Cleaning: Drop null values
cleaned_df = mysql_df.na.drop()

# Filter: Only select necessary columns
filtered_df = cleaned_df.select("column1", "column2")

# Aggregation: Calculate the sum and average
agg_df = filtered_df.groupBy("column1").agg(sum("column2").alias("total"), avg("column2").alias("average"))

# Show transformed data
agg_df.show()