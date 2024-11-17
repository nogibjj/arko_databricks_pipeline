import requests


def extract(
    url="https://raw.githubusercontent.com/MainakRepositor/Datasets/refs/heads/master/Stocks/AAPL.csv",
    file_path="dbfs:/FileStore/arko_databricks_etl/data/AAPL.csv",
):
    """ "Extract a url to a file path"""
    with requests.get(url) as r:
        dbutils.fs.put(file_path, r.content.decode('utf-8'), overwrite=True)
    return file_path

extract()
