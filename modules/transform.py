from pyspark.sql import SparkSession

# Initialize SparkSession (Databricks provides this by default)
spark = SparkSession.builder.appName("AAPL Data Transformations").getOrCreate()

def transform(catalog="ids706_data_engineering", 
             input_database="arko_inbound", 
             input_table_name="aapl_raw",
             output_database="arko_staging", 
             output_table_name="aapl_transformed"):
    """
    Reads the input table, applies transformations, and creates a new table.
    """
    try:
        # Full table names
        input_table_full_name = f"{catalog}.{input_database}.{input_table_name}"
        output_table_full_name = f"{catalog}.{output_database}.{output_table_name}"

        # Read the input table
        print(f"Reading input table from: {input_table_full_name}")
        df = spark.table(input_table_full_name)

        # Apply transformations
        print("Applying transformations...")
        transformed_df = (
            df.withColumnRenamed("close_t_", "adjusted_close")  # Renaming the 'close' column to 'adjusted_close'
              .filter(df["volume"] > 1000000)               # Example: Filter high volume
              .select("date", "open", "high", "low", "adjusted_close", "volume")  # Selecting columns
        )

        # Ensure the output database exists
        print(f"Ensuring database {output_database} exists in catalog {catalog}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{output_database}")

        # Save the transformed DataFrame as a new Delta table
        print(f"Saving transformed table to: {output_table_full_name}")
        transformed_df.write.format("delta").mode("overwrite").saveAsTable(output_table_full_name)

        print(f"Transformed table successfully created: {output_table_full_name}")
    except Exception as e:
        print("Error occurred:", e)

# Call the function
transform()
