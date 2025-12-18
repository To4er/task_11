from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

from include.config import SAKILA_URL, EXTRACT_DIR, SAKILA_SCHEMA_PATH, SAKILA_DATA_PATH, SAKILA_CONN_ID, SAKILA_ARCHIVE_PATH

default_args = {
    'owner': 'airflow',
}


@dag(
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['sakila'],
    schedule=None,
    template_searchpath=[EXTRACT_DIR]
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
            curl -L "{SAKILA_URL}" -o {SAKILA_ARCHIVE_PATH} && \
            tar -xzf {SAKILA_ARCHIVE_PATH} -C {EXTRACT_DIR}
            """,
    )
    create_schema_task = BashOperator(
        task_id='create_schema',
        bash_command=f"""
            mysql -h $DB_HOST \
                  -P $DB_PORT \
                  -u $DB_USER \
                  -e "source {EXTRACT_DIR}/{SAKILA_SCHEMA_PATH}"
            """,
        env=db_env
    )

    insert_data_task = BashOperator(
        task_id='insert_data',
        bash_command=f"""
                mysql -h $DB_HOST \
                  -P $DB_PORT \
                  -u $DB_USER \
                  -e "source {EXTRACT_DIR}/{SAKILA_DATA_PATH}"
            """,
        env=db_env,
    )

    download_files_task >> create_schema_task >> insert_data_task


sakila_setup_dag()