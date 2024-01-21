# Airflow local environment

This repository contains a docker-compose to get up and running a local Airflow environment for developing purposes.
Don't use this in production environments.

Start the environment with the `docker-compose up` command.

Be aware that in the Dockerfile, it is not specified the Airflow Docker image, so the latest version will be installed.
It is recommended to specify the same version you have in your production environment.

# Get started

## Creating dags

Add your dags in the dags folder.

## Adding variables

In file secrets/variables.yaml you can add variables to be accessed in your DAGs.

## Adding connections

In file secrets/connections.yaml you can add connections to be accessed in your DAGs.

## User and password

When the environment is ready, a user and password authentication will be prompted.
By default, use these credentials:

- user: airflow
- pass: airflow

# DAG Validator

The DAG validator is a python file that run pytest to identify unexpected orphaned airflow tasks with no/unexpected upstream and downstream dependencies. This ensures the correctness of the airflow DAGs such that the orchestrated data pipeline behaves as expected. The validation process leverages airflow DAG dependencies, expressed as relationships like `task1 >> task2`, to identify unexpected orphaned tasks with no/unexpected upstream and downstream dependencies.


![alt text](https://github.com/elliotip04/airflow-dag-validator/blob/feature/image/Airflow_DAG_Validator.jpg)

## Running tests
To execute the DAG validation tests, use the following command:<br>
```
pytest -s tests/test_dag_output.py
```
## Adding/Amending Tests
The pytest suite comprises two individual tests, each addressing specific aspects of DAG validation:

1. Find Orphaned Models with No Upstream Tasks (All layers: i.e. staging_to_source, landing-to-staging)
2. Find Incorrectly Mapped Upstream Layers (All layers: i.e. staging_to_source, landing-to-staging)

You can modify or add tests as needed to enhance the DAG validation process.

## Prerequisites

Before you begin, ensure you have met the following requirements:
- **Python:** This project requires Python. If you haven't installed it yet, you can download it from [python.org](https://www.python.org/downloads/) or use your system's package manager.

## Installation

You can install the project dependencies by running the following command:

```bash
pip install -r requirements.txt
