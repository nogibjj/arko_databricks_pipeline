from pyspark.sql import SparkSession

# Initialize SparkSession (Databricks provides this by default)
spark = SparkSession.builder.appName("AAPL Data Load").getOrCreate()

def load_to_databricks(dataset="dbfs:/FileStore/arko_databricks_etl/data/AAPL.csv", 
                       catalog="ids706_data_engineering", 
                       database="arko_inbound", 
                       table_name="aapl_raw"):
    """
    Load the CSV data as-is into a Databricks Delta table.
    """
    try:
        # Read the CSV file into a DataFrame (infer schema automatically)
        print(f"Reading CSV from: {dataset}")
        df = spark.read.csv(dataset, header=True, inferSchema=True)

        # Ensure the database exists in the specified catalog
        print(f"Ensuring database {database} exists in catalog {catalog}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

        # Save the DataFrame as a Delta table in the specified catalog and database
        table_full_name = f"{catalog}.{database}.{table_name}"
        print(f"Saving table to: {table_full_name}")
        df.write.format("delta").mode("overwrite").saveAsTable(table_full_name)

        print(f"Table successfully created: {table_full_name}")
    except Exception as e:
        print("Error occurred:", e)

# Call the function
load_to_databricks()
