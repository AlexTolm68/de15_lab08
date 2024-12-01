import os
import psycopg2
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]
PG_DATABASE = os.environ["POSTGRES_DB"]
DEFAULT_ARGS = {"owner": "lab08_team"}

CONNECTION_STRING = f'postgresql://{PG_USER}:{PG_PASSWORD}@postgres-db:5432/{PG_DATABASE}'  # 5432 внутри


# query Покупка товаров
buy_product_query = """
BEGIN;

DELETE FROM public.dm_buy_product_table
WHERE event_timestamp >= '2024-11-16 13:00:00'
    AND event_timestamp < '2024-11-16 13:00:00' + INTERVAL 1 HOUR;

INSERT INTO public.dm_buy_product_table
-- query proto
WITH
  merged_data AS (
    SELECT
      l.event_id,
      be.click_id,
      de.user_custom_id,
      page_url_path,
      be.event_timestamp::timestamp AS event_timestamp,
      -- for deduplication
      row_number() OVER (PARTITION BY l.event_id, be.click_id, de.user_custom_id,
                                      page_url_path, be.event_timestamp, event_type) AS rn
    FROM public.location_events l
    LEFT OUTER JOIN public.browser_events be ON l.event_id = be.event_id
    LEFT OUTER JOIN public.device_events de ON be.click_id = de.click_id
    WHERE date_trunc('hour', CAST(event_timestamp AS timestamp)) = '2024-11-16 13:00:00'  -- for jinja {{ date_hour }}
    ORDER BY event_timestamp),

  next_url_path_data AS (
    SELECT
      event_id,
      click_id,
      user_custom_id,
      page_url_path,
      -- to see is product was added to cart
      LEAD(page_url_path) OVER (PARTITION BY click_id, user_custom_id ORDER BY event_timestamp) AS next_url_path,
      event_timestamp
    FROM merged_data
    WHERE rn = 1
    ),

  product_buy_chain AS (
    SELECT
      click_id,
      user_custom_id,
      page_url_path,
      next_url_path,
      event_timestamp,
      -- to check if added products were successfully bought with confirmation
      MAX(CASE WHEN next_url_path = '/confirmation' THEN 1 ELSE 0 END)
        OVER (PARTITION BY click_id) AS has_confirmation
    FROM next_url_path_data
    WHERE next_url_path IN ('/cart', '/payment', '/confirmation') OR page_url_path = '/confirmation')

SELECT
  click_id,
  user_custom_id,
  page_url_path,
  event_timestamp::timestamp(0) AS event_timestamp
FROM product_buy_chain
WHERE page_url_path LIKE '/product_%'
  AND has_confirmation = 1
;

COMMIT;
"""


def postgres_execute_query(query: str, execution_date: str) -> None:
    with psycopg2.connect(CONNECTION_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def execute_sql_buy_product_update(**kwargs):
    postgres_execute_query(buy_product_query, kwargs['execution_date'])


dag = DAG(
    dag_id='data_marts_dag',
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 11, 15),
    catchup=False,
)

# wait_for_staging_done = ExternalTaskSensor(
#     external_dag_id="create_materialized_views"
# )

buy_product_update = PythonOperator(
    task_id='buy_product_update',
    python_callable=execute_sql_buy_product_update,
    provide_context=True,
    dag=dag
)

buy_product_update
