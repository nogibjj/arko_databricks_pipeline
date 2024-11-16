import requests


def extract(
    url="https://raw.githubusercontent.com/MainakRepositor/Datasets/refs/heads/master/Stocks/AAPL.csv",
    file_path="data/AAPL.csv",
):
    """ "Extract a url to a file path"""
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path
