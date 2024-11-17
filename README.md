[![CICD](https://github.com/nogibjj/arko_databricks_pipeline/actions/workflows/CICD.yml/badge.svg)](https://github.com/nogibjj/arko_databricks_pipeline/actions/workflows/CICD.yml)

# Databricks ELT Pipeline

This project demonstrates how to setup a ELT pipeline in Databricks. 
The pipeline will first extract data from a source and then load it into DBFS. Consequently, it take the DBFS file as input and then load it into catalog tables in stages (inbound -> staging -> processed) based on sequential processing.

## Pipeline Function
- A `modules/extract.py` script to extract a csv file from github and then store it in dbfs. [`extract` task]
- A `modules/load.py` script to load dbfs file into the inbound table. [`load` task]
- A `modules/transform.py` script to fetch data from inbound table, then perform transformations, and then load into the staging table. [`transform` task]
- A `modules/query.py` custom script that takes the staging table and then outputs the % change in close value for the past 5 days, into the processed table. [`query` task]


## Project Structure

- `modules/`: Contains the ELT scripts.
- `requirements.txt`: Lists the Python dependencies.
- `Makefile`: Defines common tasks like installing dependencies, running tests, linting, and running docker.
- `tests/`: Contains testing scripts.
- `.devcontainer/`: Contains `Dockerfile` and VS Code configuration.
- `.github/workflows/`: Contians CI/CD workflows for GitHub.

## Databricks setup
### 1. Create Cluster
![image](https://github.com/user-attachments/assets/31f347d4-916d-4341-a46c-0211b162672b)

Note: Install all additional packages that are required to run the pipeline.

![image](https://github.com/user-attachments/assets/abc8b842-60ce-403a-8c6e-0adf0b5e1144)


### 2. Create Catalog Schemas
![image](https://github.com/user-attachments/assets/6eaa8aa6-e4b3-4d81-80bc-b53fed46eec7)


### 3. Create Workflow
![image](https://github.com/user-attachments/assets/d0d63fe5-bd55-4cc8-8aae-2b13f93f3ac1)

Note: The source code for each individual task in the workflow is synced with this Github repo.

![image](https://github.com/user-attachments/assets/c97c3ec0-d4cd-446b-8a0e-2b9f74591728)


## Steps to update modules and deploy
### 1. Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/nogibjj/arko_databricks_pipeline
cd arko_databricks_pipeline
```

### 2. Make changes to modules and push to remote repo.

```bash
git add .
git commit -m"<your_commit_message>"
git push
```
![image](https://github.com/user-attachments/assets/62295aa5-706e-4892-9835-a3135f624bff)

### 3. Run the ELT pipeline on Databricks
![image](https://github.com/user-attachments/assets/71fd04b7-8120-4311-8be5-60e02bbd35df)
![image](https://github.com/user-attachments/assets/ab467d98-fab7-4923-949f-5521ebb83420)

## Data Source
AAPL stock data from Github
https://raw.githubusercontent.com/MainakRepositor/Datasets/refs/heads/master/Stocks/AAPL.csv

## Data Sink

Dataflow is as follows:

DBFS(file) [output from extract task] -> arko_inbound [output from load task] -> arko_staging [output from transform task] -> arko_processed [output from query task]

### Output of `extract` module:
'dbfs:/FileStore/arko_databricks_etl/data/AAPL.csv'
![image](https://github.com/user-attachments/assets/b7d22f40-a1e1-4d5a-8931-fdfdf01a9627)

### Output of `load` module:
https://dbc-c95fb6bf-a65d.cloud.databricks.com/explore/data/ids706_data_engineering/arko_inbound/aapl_raw?o=3670519680858392
![image](https://github.com/user-attachments/assets/ae543b00-de24-4dfb-ba57-4e8fa73434f8)
![image](https://github.com/user-attachments/assets/ab9fa013-ab86-45e4-9778-9c02df0d2a58)

### Output of `transform` module:
https://dbc-c95fb6bf-a65d.cloud.databricks.com/explore/data/ids706_data_engineering/arko_staging/aapl_transformed?o=3670519680858392
![image](https://github.com/user-attachments/assets/48c46829-7e70-4a70-91e6-620db556137d)
![image](https://github.com/user-attachments/assets/db9ef583-2932-4b4b-8c5a-27508c23cc2f)

## Output of `query` module:
https://dbc-c95fb6bf-a65d.cloud.databricks.com/explore/data/ids706_data_engineering/arko_processed/aapl_percent_change?o=3670519680858392
![image](https://github.com/user-attachments/assets/eb0f01eb-618d-4d7a-8949-11bbd77a3889)
![image](https://github.com/user-attachments/assets/8427a751-c7bf-437c-9099-9ba14248fe77)











