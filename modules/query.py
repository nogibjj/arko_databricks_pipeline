import sqlite3
from tabulate import tabulate


def create(entry):
    """
    Insert a new entry into the AAPL table.

    Parameters:
    - entry: A tuple containing the values to insert into the table
    """
    conn = sqlite3.connect("AAPL.db")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO AAPL VALUES (?, ?, ?, ?, ?, ?)", entry)
    conn.commit()
    conn.close()
    return "Entry added successfully!"

def read():
    conn = sqlite3.connect("AAPL.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM AAPL LIMIT 5")

    # Fetch all rows and get the column names
    results = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    # Display results as a table
    print("Query Results:")
    print(tabulate(results, headers, tablefmt="pretty"))

    conn.close()
    return "Success"

def custom_query(
    query="""WITH RecentData AS (
  SELECT
    "Close(t)" AS ClosePrice,
    DATE,
    LAG("Close(t)", 1) OVER (ORDER BY DATE DESC) AS PrevClosePrice
  FROM AAPL
  ORDER BY DATE DESC
  LIMIT 6  -- Get 6 rows to account for 5 changes
)
SELECT
  DATE,
  ClosePrice,
  PrevClosePrice,
  ROUND(((ClosePrice - PrevClosePrice) / PrevClosePrice) * 100, 2) AS PercentChange
FROM RecentData
WHERE PrevClosePrice IS NOT NULL
ORDER BY DATE DESC;
""",
):
    """
    Read the AAPL table

    Parameters: (optional)
    - entry: A string query for custom querying the table
    """
    conn = sqlite3.connect("AAPL.db")
    cursor = conn.cursor()
    cursor.execute(query)

    # Fetch all rows and get the column names
    results = cursor.fetchall()
    headers = [description[0] for description in cursor.description]

    # Display results as a table
    print("Query Results:")
    print(tabulate(results, headers, tablefmt="pretty"))

    conn.close()
    return "Success"


def update(column, new_value, condition_column, condition_value):
    """
    Update a specific value in the AAPL table.

    Parameters:
    - column: The column to update.
    - new_value: The new value to set in the column.
    - condition_column: The column to check in the WHERE condition.
    - condition_value: The value in the condition_column to match for updating.
    """
    conn = sqlite3.connect("AAPL.db")
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE AAPL SET {column} = ? WHERE {condition_column} = ?",
        (new_value, condition_value),
    )
    conn.commit()
    conn.close()
    return "Entry updated successfully!"


def delete(condition_column, condition_value):
    """
    Delete a row from the AAPL table.

    Parameters:
    - condition_column: The column to check in the WHERE condition.
    - condition_value: The value in the condition_column to match for deletion.
    """
    conn = sqlite3.connect("AAPL.db")
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM AAPL WHERE {condition_column} = ?", (condition_value,))
    conn.commit()
    conn.close()
    return "Entry deleted successfully!"
