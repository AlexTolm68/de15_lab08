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
    create_raw_device_events_table,
    raw_device_events_update_query,
    create_raw_geo_events_table,
    raw_geo_events_update_query,
    create_raw_location_events_table,
    raw_location_events_update_query,
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


def execute_sql_raw_browser_events_update(**kwargs):
    postgres_execute_query(create_raw_browser_events_table())
    postgres_execute_query(raw_browser_events_update_query(kwargs['execution_date']))


def execute_sql_raw_device_events_update(**kwargs):
    postgres_execute_query(create_raw_device_events_table())
    postgres_execute_query(raw_device_events_update_query(kwargs['execution_date']))


def execute_sql_raw_geo_events_update(**kwargs):
    postgres_execute_query(create_raw_geo_events_table())
    postgres_execute_query(raw_geo_events_update_query(kwargs['execution_date']))


def execute_sql_raw_location_events_update(**kwargs):
    postgres_execute_query(create_raw_location_events_table())
    postgres_execute_query(raw_location_events_update_query(kwargs['execution_date']))


with DAG(
    dag_id='raw_layer',
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 11, 15),
    catchup=True,
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

    raw_browser_events_update = PythonOperator(
        task_id='raw_browser_events_update',
        python_callable=execute_sql_raw_browser_events_update,
        provide_context=True,
    )

    raw_device_events_update = PythonOperator(
        task_id='raw_device_events_update',
        python_callable=execute_sql_raw_device_events_update,
        provide_context=True,
    )

    raw_geo_events_update = PythonOperator(
        task_id='raw_geo_events_update',
        python_callable=execute_sql_raw_geo_events_update,
        provide_context=True,
    )

    raw_location_events_update = PythonOperator(
        task_id='raw_location_events_update',
        python_callable=execute_sql_raw_location_events_update,
        provide_context=True,
    )

    start = EmptyOperator(task_id="start")
    completed = EmptyOperator(task_id="completed")

    start >> wait_of_browser_events >> raw_browser_events_update >> completed
    start >> wait_of_device_events >> raw_device_events_update >> completed
    start >> wait_of_geo_events >> raw_geo_events_update >> completed
    start >> wait_of_location_events >> raw_location_events_update >> completed
