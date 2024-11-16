import sqlite3
import csv
import os

# load the csv file and insert only the first 6 columns into a new sqlite3 database
def load(dataset="data/AAPL.csv"):
    """Transforms and Loads only the first 6 columns of the CSV into the local SQLite3 database"""

    # Print the full working directory and path
    print(os.getcwd())
    
    with open(dataset, newline="") as csvfile:
        payload = csv.reader(csvfile, delimiter=",")

        # Get the header from the CSV (first row contains column names)
        header = next(payload)

        # Take only the first 6 columns
        formatted_header = [f"[{col}]" for col in header[:6]]

        # Connect to (or create) the SQLite3 database
        conn = sqlite3.connect("AAPL.db")
        c = conn.cursor()

        # Drop the table if it already exists
        c.execute("DROP TABLE IF EXISTS AAPL")

        # Dynamically create the table based on the first 6 columns of the CSV header
        columns = ", ".join(formatted_header)
        create_table_query = f"CREATE TABLE AAPL ({columns})"
        c.execute(create_table_query)

        # Prepare the placeholders for the `INSERT INTO` statement
        placeholders = ", ".join(["?" for _ in formatted_header])

        # Insert the data into the table (only first 6 columns of each row)
        insert_query = f"INSERT INTO AAPL ({columns}) VALUES ({placeholders})"
        for row in payload:
            c.execute(insert_query, row[:6])  # Insert only the first 6 columns

        # Commit changes and close connection
        conn.commit()
        conn.close()

        print("CSV file has been successfully loaded into AAPL.db with only the first 6 columns")
        return "AAPL.db"
