import requests
import pandas as pd
from io import StringIO
import re
import dbutils


def sanitize_column_name(col_name):

    col_name = col_name.strip()
    col_name = col_name.lower()
    col_name = re.sub(r"[^\w\s]", "_", col_name)
    col_name = col_name.replace(" ", "_")

    return col_name


def to_dbfs(in_path, out_path):

    try:

        with open(in_path, "r") as f:
            dbutils.fs.put(out_path, f.read(), overwrite=True)
            print(f"File successfully saved to DBFS at: {out_path}")
            return out_path

    except Exception as e:
        print("Error occurred during write to dbfs:", e)
        return None


def extract_in_chunks(
    url="https://raw.githubusercontent.com/MainakRepositor/Datasets/refs/heads/master/Stocks/AAPL.csv",
    file_path="dbfs:/FileStore/arko_databricks_etl/data/AAPL.csv",
    chunk_size=10000,
):

    try:

        print(f"Downloading data from: {url}")
        response = requests.get(url)
        response.raise_for_status()

        csv_data = StringIO(response.text)
        chunk_iter = pd.read_csv(csv_data, chunksize=chunk_size)

        temp_file_path = "/tmp/AAPL_transformed.csv"
        first_chunk = True

        for chunk in chunk_iter:

            print("Sanitizing column names")
            chunk.columns = [sanitize_column_name(col) for col in chunk.columns]

            with open(temp_file_path, "a" if not first_chunk else "w") as f:

                chunk.to_csv(f, index=False, header=first_chunk)
                first_chunk = False

            print(f"Processed chunk written to: {temp_file_path}")

        return temp_file_path, file_path

    except Exception as e:
        print("Error occurred during extraction:", e)
        return None


def main():
    temp_path, out_path = extract_in_chunks()
    to_dbfs(temp_path, out_path)


main()
