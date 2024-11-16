[![Install](https://github.com/nogibjj/arko-sqlite-lab/actions/workflows/install.yml/badge.svg)](https://github.com/nogibjj/arko-sqlite-lab/actions/workflows/install.yml)
[![Format](https://github.com/nogibjj/arko-sqlite-lab/actions/workflows/format.yml/badge.svg)](https://github.com/nogibjj/arko-sqlite-lab/actions/workflows/format.yml)
[![Lint](https://github.com/nogibjj/arko-sqlite-lab/actions/workflows/lint.yml/badge.svg)](https://github.com/nogibjj/arko-sqlite-lab/actions/workflows/lint.yml)
[![Test](https://github.com/nogibjj/arko-sqlite-lab/actions/workflows/test.yml/badge.svg)](https://github.com/nogibjj/arko-sqlite-lab/actions/workflows/test.yml)
[![Deploy](https://github.com/nogibjj/arko-sqlite-lab/actions/workflows/deploy.yml/badge.svg)](https://github.com/nogibjj/arko-sqlite-lab/actions/workflows/deploy.yml)

# CLI Tool to interact with SQLite Database

This project is to demonstrate how to perform ETL processes on a dataset and creating a CLI tool enable users to interact with the database.

## Project Function
- A `main.py` script which serves as the CLI structure and logic.
- A `mylib/extract.py` script to extract a csv file from github.
- A `mylib/transform_load.py` script to remove all unwanted columns and then load it to a sqlite database
- A `mylib/query.py` script to perform CRUD queries on the dataset along with the option to execute custom queries.<br>Default custom script outputs the % change in close value for the past 5 days.
- A `test_main.py` script to unit test the different functionalities.

![image](https://github.com/user-attachments/assets/fc27b7c0-e55d-45d5-9cd0-b92fcad6eb6e)



## Project Structure

- `mylib/`: Contains the ETL scripts.
- `requirements.txt`: Lists the Python dependencies.
- `Makefile`: Defines common tasks like installing dependencies, running tests, linting, and running docker.
- `.devcontainer/`: Contains `Dockerfile` and VS Code configuration.
- `.github/workflows/`: Contians CI/CD workflows for GitHub.
![image](https://github.com/user-attachments/assets/86b856c0-d55a-486e-8811-984f8011456e)

## Project Setup
### 1. Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/nogibjj/arko-sqlite-lab
cd arko-sqlite-lab
```

### 2. Run CLI tool

```bash
.venv/bin/python main.py
```
![image](https://github.com/user-attachments/assets/107a7c06-d6ca-415a-b94a-e0b0b3b3e7c1)
![image](https://github.com/user-attachments/assets/bbeadc3f-314c-40e0-a141-ebb3ba233a30)



