from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Custom Query Execution").getOrCreate()


def custom_query(
    query="""WITH RecentData AS (
              SELECT
                `adjusted_close` AS ClosePrice,  -- Adjusted to use sanitized column name
                `date`,
                LAG(`adjusted_close`, 1) OVER (ORDER BY `date` DESC) AS PrevClosePrice
              FROM arko_staging.aapl_transformed
              ORDER BY `date` DESC
              LIMIT 6  -- Get 6 rows to account for 5 changes
            )
            SELECT
              `date`,
              ClosePrice,
              PrevClosePrice,
              ROUND(((ClosePrice - PrevClosePrice) / PrevClosePrice) * 100, 2) AS PercentChange
            FROM RecentData
            WHERE PrevClosePrice IS NOT NULL
            ORDER BY `date` DESC;
        """,
    catalog="ids706_data_engineering",
    input_database="arko_staging",
    output_database="arko_processed",
    output_table_name="aapl_percent_change",
):

    try:

        input_table_full_name = f"{catalog}.{input_database}.aapl_data"
        output_table_full_name = f"{catalog}.{output_database}.{output_table_name}"

        print(f"Executing query on: {input_table_full_name}")
        result_df = spark.sql(query)

        print(f"Ensuring database {output_database} exists in catalog {catalog}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{output_database}")

        print(f"Saving result table to: {output_table_full_name}")
        result_df.write.format("delta").mode("overwrite").saveAsTable(
            output_table_full_name
        )

        print(f"Query result successfully saved to table: {output_table_full_name}")

    except Exception as e:
        print("Error occurred:", e)


custom_query()
