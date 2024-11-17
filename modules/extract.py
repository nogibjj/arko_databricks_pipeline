import requests
import pandas as pd
from io import StringIO
import re

def sanitize_column_name(col_name):
    """
    Sanitize column names to conform to SQL naming conventions:
    - Convert to lowercase
    - Replace spaces and special characters with underscores
    - Remove any leading or trailing spaces
    - Remove square brackets ([]), curly brackets ({}), round brackets (())
    """
    # Strip leading/trailing spaces
    col_name = col_name.strip()

    # Convert to lowercase
    col_name = col_name.lower()

    # Replace any invalid characters with underscores
    # This removes commas, semicolons, curly and round brackets, newlines, tabs, and equals sign
    col_name = re.sub(r'[^\w\s]', '_', col_name)  # Replace non-word and non-space characters with underscore

    # Replace spaces with underscores
    col_name = col_name.replace(" ", "_")

    return col_name

def extract_in_chunks(
    url="https://raw.githubusercontent.com/MainakRepositor/Datasets/refs/heads/master/Stocks/AAPL.csv",
    file_path="dbfs:/FileStore/arko_databricks_etl/data/AAPL.csv",
    chunk_size=10000
):
    """
    Extracts CSV data from a URL, processes it in chunks, sanitizes all column names,
    and writes the processed data to DBFS.
    """
    try:
        # Download the data from the URL
        print(f"Downloading data from: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses

        # Read the CSV in chunks using pandas
        csv_data = StringIO(response.text)
        chunk_iter = pd.read_csv(csv_data, chunksize=chunk_size)

        # Temporary path for writing the processed chunks
        temp_file_path = "/tmp/AAPL_transformed.csv"
        first_chunk = True  # Flag to write header only once

        for chunk in chunk_iter:
            # Sanitize all column names
            print("Sanitizing column names")
            chunk.columns = [sanitize_column_name(col) for col in chunk.columns]

            # Append or write the chunk to the DBFS file
            with open(temp_file_path, "a" if not first_chunk else "w") as f:
                # Write header only for the first chunk
                chunk.to_csv(f, index=False, header=first_chunk)
                first_chunk = False

            print(f"Processed chunk written to: {temp_file_path}")

        # Now upload the final file to DBFS
        with open(temp_file_path, "r") as f:
            dbutils.fs.put(file_path, f.read(), overwrite=True)

        print(f"File successfully saved to DBFS at: {file_path}")
        return file_path

    except Exception as e:
        print("Error occurred during extraction:", e)
        return None

# Call the function
extract_in_chunks()
