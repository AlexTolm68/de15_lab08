import os
import psycopg2
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from dag_utils.query_dm_dist_events import create_dist_events_table, query_dist_events
 
 
PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]
PG_DATABASE = os.environ["POSTGRES_DB"]
DEFAULT_ARGS = {"owner": "lab08_team"}
 
CONNECTION_STRING = f'postgresql://{PG_USER}:{PG_PASSWORD}@postgres-db:5432/{PG_DATABASE}'  # 5432 внутри
 
 
def postgres_execute_query(query: str) -> None:
    with psycopg2.connect(CONNECTION_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
 
 
def execute_sql_dist_events_count(**context):
    postgres_execute_query(create_dist_events_table())
    postgres_execute_query(query_dist_events(context['execution_date']))
 
 
with DAG(
    dag_id='dm_dist_events',
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 11, 15),
    end_date=pendulum.datetime(2024, 11, 27),
    max_active_runs=3,
    catchup=True,
) as dag:
 
    wait_of_core_layer_dag = ExternalTaskSensor(
        task_id="wait_of_core_layer_dag", external_dag_id="data_core_update", external_task_id="completed"
    )
 
    dist_events_count = PythonOperator(
        task_id='dm_dist_events_count',
        python_callable=execute_sql_dist_events_count,
        provide_context=True,
    )
 
    start = EmptyOperator(task_id="start")
    completed = EmptyOperator(task_id="completed")
 
    start >> wait_of_core_layer_dag >> dist_events_count >> completed
    