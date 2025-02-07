# Import variables
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

from Modules.extract import extract_data, get_last_loaded_date
from Modules.helper import connect_clickhouse, connect_postgresql
from Modules.load import load_data, move_data
from dotenv import load_dotenv

engine = connect_clickhouse()
db_engine = connect_postgresql()
load_dotenv(override=True)

# Get max date from DB
max_date = get_last_loaded_date(db_engine=db_engine)

sql_query = f"""
            SELECT * FROM tripdata 
            WHERE pickup_date = toDate('{max_date}') + 1
            """

# Setting default parameters
default_args = {
    'owner' : 'toluse',
    'start_date' : datetime(year=2024, month=11, day=15),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries': None,
    #'retry_delay' : timedelta(minutes=10)
}

# Instantiate the DAG
with DAG(
    'Clickhouse_ETL',
    default_args = default_args,
    description = 'An ETL Pipeline for Divytrips trip data from clickhouse to PostgreSQL DB',
    schedule_interval = '0 0 * * *',
    catchup = False
) as dag:
    # Define task 1
    start_task = DummyOperator(
        task_id = 'Pipeline_start'
    )

    # Define task 2
    extract_task = PythonOperator(
        task_id = 'extract',
        python_callable = extract_data,
        op_kwargs = {'sql': sql_query, 'client': engine}
    )

    # Define task 3
    staging_load_task = PythonOperator(
        task_id = 'stg_load',
        python_callable = load_data,
        op_kwargs = {'schema': 'STG', 'engine': db_engine}
    )

    # Define task 4
    staging_move_task = PythonOperator(
        task_id = 'stg_move',
        python_callable = move_data,
        op_kwargs = {'engine': db_engine}
    )

    # Define task 5
    end_task = DummyOperator(
        task_id = 'End_Pipeline'
    )

# Set dependencies
start_task >> extract_task >> staging_load_task >> staging_move_task >> end_task
