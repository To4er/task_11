from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator

from include.config import PAGILA_SCHEMA_URL, PAGILA_SCHEMA_PATH, PAGILA_DATA_URL, PAGILA_DATA_PATH, EXTRACT_DIR, PAGILA_CONN_ID, PAGILA_FOLDER

default_args = {
    'owner': 'airflow',
}

@dag(
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['pagila'],
    schedule=None,
    template_searchpath=[EXTRACT_DIR]
)
def pagila_setup_dag():
    download_schema_task = BashOperator(
        task_id='download_schema_file',
        bash_command=f"mkdir -p {EXTRACT_DIR}/{PAGILA_FOLDER} && curl -o {EXTRACT_DIR}/{PAGILA_SCHEMA_PATH} {PAGILA_SCHEMA_URL}"
    )

    download_data_task = BashOperator(
        task_id='download_data_file',
        bash_command=f"curl -o {EXTRACT_DIR}/{PAGILA_DATA_PATH} {PAGILA_DATA_URL}"
    )

    cleanup_db = SQLExecuteQueryOperator(
        task_id='cleanup_db',
        conn_id=PAGILA_CONN_ID,
        sql="DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;",
        split_statements=True
    )

    create_schema_task = SQLExecuteQueryOperator(
        task_id='pagila_create_schema_task',
        conn_id=PAGILA_CONN_ID,
        sql=PAGILA_SCHEMA_PATH,
        split_statements=True
    )

    insert_data_task = SQLExecuteQueryOperator(
        task_id='pagila_insert_data_task',
        conn_id=PAGILA_CONN_ID,
        sql=PAGILA_DATA_PATH,
        split_statements=True
    )

    [download_schema_task, download_data_task] >> cleanup_db >> create_schema_task >> insert_data_task

pagila_setup_dag()