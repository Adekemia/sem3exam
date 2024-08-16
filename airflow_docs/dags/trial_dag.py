from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
from datetime import timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'pg_load_bq',
    default_args=default_args,
    description='ETL process from PostgreSQL to BigQuery',
    schedule_interval=None,  # Change to a cron expression if needed
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def get_postgres_credentials():
        """Retrieve PostgreSQL credentials from Airflow Variables."""
        postgres_conn_id = Variable.get("postgres_conn_id")
        postgres_host = Variable.get("postgres_host")
        postgres_port = Variable.get("postgres_port")
        postgres_user = Variable.get("postgres_user")
        postgres_password = Variable.get("postgres_password")
        postgres_db = Variable.get("postgres_db")
        return {
            'conn_id': postgres_conn_id,
            'host': postgres_host,
            'port': postgres_port,
            'user': postgres_user,
            'password': postgres_password,
            'db': postgres_db
        }

    def get_bigquery_credentials():
        """Retrieve BigQuery credentials from Airflow Variables."""
        bigquery_conn_id = Variable.get("bigquery_conn_id")
        dataset_id = Variable.get("bigquery_dataset_id")
        return {
            'conn_id': bigquery_conn_id,
            'dataset_id': dataset_id
        }

    def extract_data_from_postgres(table_name):
        """Extract data from a PostgreSQL table."""
        creds = get_postgres_credentials()
        pg_hook = PostgresHook(
            postgres_conn_id=creds['conn_id'],
            host=creds['host'],
            schema=creds['db'],
            login=creds['user'],
            password=creds['password'],
            port=creds['port']
        )
        query = f"SELECT * FROM {table_name}"
        df = pg_hook.get_pandas_df(query)
        return df

    def transform_data(df):
        """Transform data if necessary."""
        # Add transformation logic here if needed
        return df

    def load_data_into_bigquery(df, table_name):
        """Load data into Google BigQuery."""
        creds = get_bigquery_credentials()
        bq_hook = BigQueryHook(bigquery_conn_id=creds['conn_id'])
        table_id = f"{creds['dataset_id']}.{table_name}"
        bq_hook.insert_rows(table_id=table_id, rows=df.to_dict('records'))

    def etl_process(table_name):
        """Perform the ETL process for a given table."""
        # Extract
        df = extract_data_from_postgres(table_name)
        # Transform
        df = transform_data(df)
        # Load
        load_data_into_bigquery(df, table_name)

    def list_tables():
        """List all tables in the PostgreSQL database."""
        creds = get_postgres_credentials()
        pg_hook = PostgresHook(
            postgres_conn_id=creds['conn_id'],
            host=creds['host'],
            schema=creds['db'],
            login=creds['user'],
            password=creds['password'],
            port=creds['port']
        )
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
        tables = cursor.fetchall()
        return [table[0] for table in tables]

    def create_etl_tasks():
        """Create ETL tasks for each table dynamically."""
        tables = list_tables()
        for table in tables:
            task = PythonOperator(
                task_id=f'etl_{table}',
                python_callable=etl_process,
                op_args=[table],
                dag=dag
            )
            task

    # Define a task to create ETL tasks dynamically
    create_etl_tasks_task = PythonOperator(
        task_id='create_etl_tasks',
        python_callable=create_etl_tasks,
        dag=dag
    )
