from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("AAPL Data Load").getOrCreate()

def load_to_databricks(dataset="dbfs:/FileStore/arko_databricks_etl/data/AAPL.csv", 
                       catalog="ids706_data_engineering", 
                       database="arko_aapl", 
                       table_name="aapl_data"):
    """Transforms and Loads only the first 6 columns of the CSV into a Databricks table"""

    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", IntegerType(), True)
    ])

    try:

        print(f"Reading CSV from: {dataset}")
        df = spark.read.csv(dataset, header=True, schema=schema)

        print(f"Ensuring database {database} exists in catalog {catalog}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

        table_full_name = f"{catalog}.{database}.{table_name}"
        print(f"Saving table to: {table_full_name}")
        df.write.format("delta").mode("overwrite").saveAsTable(table_full_name)

        print(f"Table successfully created: {table_full_name}")
    except Exception as e:
        print("Error occurred:", e)

load_to_databricks()
