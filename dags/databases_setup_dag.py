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
    tags=['databases'],
    schedule=None
)
def databases_setup_dag():

    pagila_create_schema_task = SQLExecuteQueryOperator(
        task_id='pagila_create_schema_task',
        conn_id='postgres_pagila_conn',
        sql="ddl/pagila-schema.sql"
    )

    pagila_insert_data_task = SQLExecuteQueryOperator(
        task_id='pagila_insert_data_task',
        conn_id='postgres_pagila_conn',
        sql="ddl/pagila-insert-data.sql"
    )

    sakila_create_schema_task = SQLExecuteQueryOperator(
        task_id='sakila_create_schema_task',
        conn_id='mysql_sakila_conn_id',
        sql="ddl/sakila-schema.sql",
        split_statements=True
    )

    sakila_insert_task = SQLExecuteQueryOperator(
        task_id='sakila_insert_data_task',
        conn_id='mysql_sakila_conn_id',
        sql="ddl/sakila-data.sql",
        split_statements=True
    )

    pagila_create_schema_task >> pagila_insert_data_task
    sakila_create_schema_task >> sakila_insert_task

databases_setup_dag()