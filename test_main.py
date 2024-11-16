import sqlite3
from mylib.query import create, read, update, delete


# Helper function to reset the database
def reset_database():
    conn = sqlite3.connect("AAPL.db")
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS AAPL")
    cursor.execute(
        """
        CREATE TABLE AAPL (
            DATE TEXT,
            OPEN REAL,
            HIGH REAL,
            LOW REAL,
            "Close(t)" REAL,
            VOLUME INTEGER
        )
    """
    )
    conn.commit()
    conn.close()


# Test creating an entry
def test_create_entry():
    reset_database()
    entry = ("2024-10-06", 100.0, 110.0, 95.0, 105.0, 100000)
    result = create(entry)
    assert result == "Entry added successfully!"

    # Check if the entry exists
    conn = sqlite3.connect("AAPL.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM AAPL WHERE DATE = '2024-10-06'")
    data = cursor.fetchone()
    conn.close()

    assert data == entry, f"Expected {entry}, but got {data}"


# Test reading entries
def test_read_entries():
    reset_database()
    entry = ("2024-10-06", 100.0, 110.0, 95.0, 105.0, 100000)
    create(entry)

    result = read()
    print("result", result)
    assert result == "Success"  # Check if the default query result output is present


# Test updating an entry
def test_update_entry():
    reset_database()
    entry = ("2024-10-06", 100.0, 110.0, 95.0, 105.0, 100000)
    create(entry)

    # Update the entry
    result = update("OPEN", 120.0, "DATE", "2024-10-06")
    assert result == "Entry updated successfully!"

    # Check if the update was successful
    conn = sqlite3.connect("AAPL.db")
    cursor = conn.cursor()
    cursor.execute("SELECT OPEN FROM AAPL WHERE DATE = '2024-10-06'")
    data = cursor.fetchone()
    conn.close()

    assert data[0] == 120.0, f"Expected OPEN = 120.0, but got {data[0]}"


# Test deleting an entry
def test_delete_entry():
    reset_database()
    entry = ("2024-10-06", 100.0, 110.0, 95.0, 105.0, 100000)
    create(entry)

    # Delete the entry
    result = delete("DATE", "2024-10-06")
    assert result == "Entry deleted successfully!"

    # Check if the entry was deleted
    conn = sqlite3.connect("AAPL.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM AAPL WHERE DATE = '2024-10-06'")
    data = cursor.fetchone()
    conn.close()

    assert data is None, f"Expected None, but got {data}"


# Run all tests
if __name__ == "__main__":
    test_create_entry()
    test_read_entries()
    test_update_entry()
    test_delete_entry()

    print("All tests passed!")
