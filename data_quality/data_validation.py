import great_expectations as ge

# Load data into Great Expectations DataFrame
ge_df = ge.dataset.SparkDFDataset(spark_df)

# Define expectations
ge_df.expect_column_values_to_not_be_null("column1")
ge_df.expect_column_values_to_be_in_set("column2", ["value1", "value2"])

# Validate data
results = ge_df.validate()

# Print validation results
print(results)