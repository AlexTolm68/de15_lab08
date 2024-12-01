import os
import psycopg2
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_utils.data_marts_queries import create_buy_product_table, buy_product_query


PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]
PG_DATABASE = os.environ["POSTGRES_DB"]
DEFAULT_ARGS = {"owner": "lab08_team"}

CONNECTION_STRING = f'postgresql://{PG_USER}:{PG_PASSWORD}@postgres-db:5432/{PG_DATABASE}'  # 5432 внутри


def postgres_execute_query(query: str) -> None:
    with psycopg2.connect(CONNECTION_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def execute_sql_buy_product_update(**kwargs):
    postgres_execute_query(create_buy_product_table())
    postgres_execute_query(buy_product_query(kwargs['execution_date']))


dag = DAG(
    dag_id='data_marts_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 11, 15),
    catchup=False,
)

buy_product_update = PythonOperator(
    task_id='buy_product_update',
    python_callable=execute_sql_buy_product_update,
    provide_context=True,
    dag=dag
)

buy_product_update
