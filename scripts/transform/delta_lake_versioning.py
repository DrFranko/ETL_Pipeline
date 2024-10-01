# Enable Delta Lake
from delta import DeltaTable

# Save data as Delta Lake format
enriched_df.write.format("delta").mode("overwrite").save("/path/to/delta-lake")

# Load data from Delta Lake
delta_df = spark.read.format("delta").load("/path/to/delta-lake")
delta_df.show()

# Update Delta Lake data
delta_table = DeltaTable.forPath(spark, "/path/to/delta-lake")
delta_table.update(col("id") == 1, {"column": "'new_value'"})