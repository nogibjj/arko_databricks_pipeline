import os
from unittest.mock import patch, Mock
from modules.extract import (
    extract_in_chunks,
)  # Adjust this import based on your file/module structure

# Mock CSV data to simulate downloading a file
mock_csv_data = """Date,Open,High,Low,Close,Adj Close,Volume
2021-01-01,133.72,133.99,131.72,133.72,133.72,155510900
2021-01-04,129.41,133.46,128.13,131.01,131.01,143301900
2021-01-05,131.01,133.41,129.99,131.01,131.01,100510900
"""


# Mock the sanitize_column_name function (you may want to import it or mock it here too)
def mock_sanitize_column_name(col_name):
    return col_name.strip().lower().replace(" ", "_").replace(",", "")


# Mock the requests.get method to return the mocked CSV data
@patch("requests.get")
def test_extract_in_chunks(mock_get):
    # Mock the response from requests.get
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = mock_csv_data
    mock_get.return_value = mock_response

    # Call the extract_in_chunks function
    temp_file_path, out_path = extract_in_chunks(
        url="https://example.com/fake_url", chunk_size=2
    )

    # Validate the result: Ensure the correct file path is returned
    assert (
        temp_file_path == "/tmp/AAPL_transformed.csv"
    ), f"Expected /tmp/AAPL_transformed.csv, but got {temp_file_path}"

    # Check if the file was created locally
    file_exists = os.path.exists(temp_file_path)
    assert file_exists, f"File not found at {temp_file_path}"

    # Validate the content of the file (the file should have sanitized headers and data)
    with open(temp_file_path, "r") as f:
        lines = f.readlines()
        assert len(lines) > 1, "File seems empty or not properly written"
        assert "date" in lines[0].lower(), "Column names were not sanitized correctly"

    os.remove(temp_file_path)

    print("Test passed successfully!")


if __name__ == "__main__":
    test_extract_in_chunks()
