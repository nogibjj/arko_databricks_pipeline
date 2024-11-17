from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("AAPL Data Transformations").getOrCreate()


def transform(
    catalog="ids706_data_engineering",
    input_database="arko_inbound",
    input_table_name="aapl_raw",
    output_database="arko_staging",
    output_table_name="aapl_transformed",
):

    try:

        input_table_full_name = f"{catalog}.{input_database}.{input_table_name}"
        output_table_full_name = f"{catalog}.{output_database}.{output_table_name}"

        print(f"Reading input table from: {input_table_full_name}")
        df = spark.table(input_table_full_name)

        print("Applying transformations...")
        transformed_df = (
            df.withColumnRenamed("close_t_", "adjusted_close")
            .filter(df["volume"] > 1000000)
            .select("date", "open", "high", "low", "adjusted_close", "volume")
        )

        print(f"Ensuring database {output_database} exists in catalog {catalog}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{output_database}")

        print(f"Saving transformed table to: {output_table_full_name}")
        transformed_df.write.format("delta").mode("overwrite").saveAsTable(
            output_table_full_name
        )

        print(f"Transformed table successfully created: {output_table_full_name}")
    except Exception as e:
        print("Error occurred:", e)


transform()
