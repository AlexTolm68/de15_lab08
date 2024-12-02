import os
import psycopg2
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from dag_utils.raw_queries import (
    create_raw_browser_events_table,
    raw_browser_events_update_query,
)

PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]
PG_DATABASE = os.environ["POSTGRES_DB"]
DEFAULT_ARGS = {"owner": "lab08_team"}

CONNECTION_STRING = f'postgresql://{PG_USER}:{PG_PASSWORD}@postgres-db:5432/{PG_DATABASE}'  # 5432 внутри


def postgres_execute_query(query: str) -> None:
    with psycopg2.connect(CONNECTION_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def execute_sql_raw_raw_browser_events_update(**kwargs):
    postgres_execute_query(create_raw_browser_events_table())
    postgres_execute_query(raw_browser_events_update_query(kwargs['execution_date']))


with DAG(
    dag_id='raw_layer',
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 11, 15),
    catchup=False,
) as dag:

    wait_of_browser_events = ExternalTaskSensor(
        task_id="wait_of_browser_events", external_dag_id="lab08_browser_events", external_task_id="grab_s3_data"
    )
    wait_of_device_events = ExternalTaskSensor(
        task_id="wait_of_device_events", external_dag_id="lab08_device_events", external_task_id="grab_s3_data"
    )
    wait_of_geo_events = ExternalTaskSensor(
        task_id="wait_of_geo_events", external_dag_id="lab08_geo_events", external_task_id="grab_s3_data"
    )
    wait_of_location_events = ExternalTaskSensor(
        task_id="wait_of_location_events", external_dag_id="lab08_location_events", external_task_id="grab_s3_data",
    )

    staging_update = PythonOperator(
        task_id='raw_update',
        python_callable=execute_sql_raw_raw_browser_events_update,
        provide_context=True,
    )

    start = EmptyOperator(task_id="start")
    wait_for_dependencies = EmptyOperator(task_id="wait_for_dependencies")
    completed = EmptyOperator(task_id="completed")

    start >> wait_of_browser_events >> wait_for_dependencies
    start >> wait_of_device_events >> wait_for_dependencies
    start >> wait_of_geo_events >> wait_for_dependencies
    start >> wait_of_location_events >> wait_for_dependencies
    wait_for_dependencies >> staging_update >> completed
