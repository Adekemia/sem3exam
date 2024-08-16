from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
import pandas as pd
import psycopg2

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 16),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'dy_postgres_to_bigquery_etl',
    default_args=default_args,
    description='ETL from PostgreSQL to BigQuery with dynamic table handling',
    schedule_interval=None,  # Run the DAG manually or schedule it as needed
)

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

#def list_tables():
    #"""List all tables in the PostgreSQL database."""

"""""

    # Retrieve connection ID from Airflow variables
    conn_id = Variable.get("postgres_conn_id", default_var=None)

    # Ensure conn_id is not None
    if conn_id is None:
        raise ValueError("The postgres_conn_id variable is not set in Airflow Variables.")


    # Initialize the PostgresHook
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Execute the query to list tables
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cursor.fetchall()

    # Return the list of table names
    return [table[0] for table in tables]
"""


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
    
    
def extract_load_table(table_name):
    """Extract a table from PostgreSQL and load it into BigQuery."""
    conn_id = Variable.get("postgres_conn_id", default_var=None)

    # Ensure conn_id is not None
    if conn_id is None:
        raise ValueError("The postgres_conn_id variable is not set in Airflow Variables.")

    # Initialize the PostgresHook
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    df = pg_hook.get_pandas_df(f"SELECT * FROM {table_name}")
    
    # Load into BigQuery
    bq_hook = BigQueryHook(bigquery_conn_id='bigquery_default', use_legacy_sql=False)
    bq_client = bq_hook.get_client()
    
    # Define the table reference in BigQuery
    dataset_id = "sem3exam.ecommerce_data"
    table_id = table_name
    
    # Load DataFrame to BigQuery
    job = bq_client.load_table_from_dataframe(df, f"{dataset_id}.{table_id}")
    job.result()  # Wait for the job to complete

def create_dynamic_tasks(**kwargs):
    """Create dynamic tasks for each table."""
    tables = list_tables()
    
    for table_name in tables:
        task = PythonOperator(
            task_id=f'load_{table_name}_to_bigquery',
            python_callable=extract_load_table,
            op_kwargs={'table_name': table_name},
            dag=dag,
        )
        start >> task >> end

# Define the start and end tasks
start = PythonOperator(
    task_id='start',
    python_callable=lambda: print("Starting ETL process"),
    dag=dag,
)

end = PythonOperator(
    task_id='end',
    python_callable=lambda: print("ETL process completed"),
    dag=dag,
)

# Define the task to create dynamic tasks
create_tasks = PythonOperator(
    task_id='create_dynamic_tasks',
    python_callable=create_dynamic_tasks,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
start >> create_tasks >> end
