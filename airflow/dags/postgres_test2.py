import os
import psycopg2
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]
PG_DATABASE = os.environ["POSTGRES_DB"]
DEFAULT_ARGS = {"owner": "lab08_team"}


dag = DAG(
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 11, 15),
    catchup=False,
)

def postgres_test_query():
    connection_string = f'postgresql://{PG_USER}:{PG_PASSWORD}@postgres-db:5432/{PG_DATABASE}'
    query = 'CREATE TABLE if not exist public.test_table (my_column varchar(10));'
    
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result[0]



test_query_task = PythonOperator(
    task_id='read',
    python_callable=postgres_test_query,
    provide_context=True,
    dag=dag
)

test_query_task
