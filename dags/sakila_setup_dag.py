from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
from include.config import SAKILA_URL, EXTRACT_DIR, SAKILA_SCHEMA_FILE, SAKILA_DATA_FILE, SAKILA_CONN_ID

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

    db_env = {
        'DB_HOST': f'{{{{ conn.{SAKILA_CONN_ID}.host }}}}',
        'DB_PORT': f'{{{{ conn.{SAKILA_CONN_ID}.port }}}}',
        'DB_USER': f'{{{{ conn.{SAKILA_CONN_ID}.login }}}}',
        'MYSQL_PWD': f'{{{{ conn.{SAKILA_CONN_ID}.password }}}}'
    }

    download_files_task = BashOperator(
        task_id='download_files',
        bash_command=f"""
            curl -L "{SAKILA_URL}" -o /tmp/sakila-db.tar.gz && \
            tar -xzf /tmp/sakila-db.tar.gz -C {EXTRACT_DIR}
            """,
    )
    create_schema_task = BashOperator(
        task_id='create_schema',
        bash_command=f"""
            mysql -h $DB_HOST \
                  -P $DB_PORT \
                  -u $DB_USER \
                  -e "source {SAKILA_SCHEMA_FILE}"
            """,
        env=db_env
    )

    insert_data_task = BashOperator(
        task_id='insert_data',
        bash_command=f"""
                mysql -h $DB_HOST \
                  -P $DB_PORT \
                  -u $DB_USER \
                  -e "source {SAKILA_DATA_FILE}"
            """,
        env=db_env,
    )

    download_files_task >> create_schema_task >> insert_data_task


sakila_setup_dag()