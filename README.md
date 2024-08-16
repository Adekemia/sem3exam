# End-to-End Data Pipeline
This is a pipleine that extracts data from a Postgres database (source) and loads it into a datawarehouse (BigQuery)
## Prepare source: Setting up Basic Postgres Infrastructure with Docker  
This step involves ingesting data into postgres for the POC  
## Set up air flow 
Start up airflow using a docker compose file

## Create DAG
Purpose built python scripts contain DAG instantiation with instructions for for extract and load operations

## Set up DBT for transformation

## How to run
- Fork this repo from the remote reposity
- Use git clone alt-sch-de-portfolio on your local device
- Use git checkout -b {a-branch-name-of-your-choice} to create and checkout to a new branch
Run docker app on your laptop
    - Use docker pull to download the image from the doker registry
- Type 'docker compose up' in your terminal (preferably vscode) 
    - This should run the postgres image

### Running Airflow for Windows
Set airflow home: set AIRFLOW_HOME=.
Initialise airflow: docker-compose up airflow-init
Run webserver and scheduler: docker compose run airflow-webserver airflow db init
- docker compose up -d airflow-webserver
- docker compose up -d airflow-scheduler


### Setting up the virtual environment
*Install*: pip install virtualenv  
*Create*: python -m venv 'environment-name' 

- Run main.py python script in the src folder on the terminal: _python main.py_
    -  This executes the queries in the python scripts


## Resources
See [Docker web resources](https://docs.docker.com/guides/walkthroughs/run-a-container/) for assistance with setting up postgres image


