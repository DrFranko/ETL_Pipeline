from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define UDF for enrichment
def enrich_data(value):
    return "Enriched: " + value

enrich_udf = udf(enrich_data, StringType())

# Join with another DataFrame
joined_df = mysql_df.join(parquet_df, mysql_df.id == parquet_df.id, "inner")

# Apply UDF to enrich data
enriched_df = joined_df.withColumn("enriched_column", enrich_udf(col("column_to_enrich")))

# Show enriched data
enriched_df.show()