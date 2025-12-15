from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
}

@dag(
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['pagila'],
    schedule=None
)
def pagila_setup_dag():

    create_schema_task = SQLExecuteQueryOperator(
        task_id='pagila_create_schema_task',
        conn_id='postgres_pagila_conn',
        sql="sql/pagila-schema.sql"
    )

    insert_data_task = SQLExecuteQueryOperator(
        task_id='pagila_insert_data_task',
        conn_id='postgres_pagila_conn',
        sql="sql/pagila-insert-data.sql"
    )

    create_schema_task >> insert_data_task

pagila_setup_dag()