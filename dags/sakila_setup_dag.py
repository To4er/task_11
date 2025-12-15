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
    tags=['sakila'],
    schedule=None
)
def sakila_setup_dag():

    create_schema_task = SQLExecuteQueryOperator(
        task_id='sakila_create_schema_task',
        conn_id='mysql_sakila_conn_id',
        sql="sql/sakila-schema.sql",
        split_statements=True
    )

    insert_data_task = SQLExecuteQueryOperator(
        task_id='sakila_insert_data_task',
        conn_id='mysql_sakila_conn_id',
        sql="sql/sakila-data.sql",
        split_statements=True
    )

    create_schema_task >> insert_data_task

sakila_setup_dag()