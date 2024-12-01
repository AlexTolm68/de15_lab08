import os
import psycopg2
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]
PG_DATABASE = os.environ["POSTGRES_DB"]
DEFAULT_ARGS = {"owner": "lab08_team"}

CONNECTION_STRING = f'postgresql://{PG_USER}:{PG_PASSWORD}@postgres-db:5432/{PG_DATABASE}' # 5432 внутри

# Распределение событий по часам (столбики)
QUERY_HOURLY_EVENTS = """
    create materialized view IF NOT EXISTS public.hourly_events as
    select 
        date_trunc('hour', event_timestamp::timestamp) as event_hour,
        count(*) as event_count
    from public.browser_events
    group by date_trunc('hour', event_timestamp::timestamp)
    order by event_hour asc;
"""

# Количество купленных товаров в разрезе часа (либо таблица, либо пироги)
QUERY_HOURLY_PURCHARES ="""
create materialized view IF NOT EXISTS public.hourly_purchares as
select 
	date_trunc('hour', event_timestamp::timestamp) as event_hour,
	count(*) as purshares_count
from public.browser_events be
join public.location_events le on be.event_id=le.event_id
where page_url_path = '/confirmation'
group by date_trunc('hour', event_timestamp::timestamp)
order by event_hour asc;
"""

# Топ-10 посещённых страниц, с которых был переход в покупку — список ссылок с количеством покупок
QUERY_TOP10_SOURCES = """
create materialized view IF NOT EXISTS public.top10_sources as
select referer_url, count(*) as cnt
from public.browser_events be
join public.location_events le on be.event_id = le.event_id
where page_url_path = '/confirmation'
group by referer_url
order by cnt desc
limit 10;
"""


dag = DAG(
    dag_id = 'create_materialized_views',
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2024, 11, 15),
    catchup=False,
)


def postgres_execute_query(query:str) -> None:
    with psycopg2.connect(CONNECTION_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def create_materialized_view() -> None:
    postgres_execute_query(QUERY_HOURLY_EVENTS)
    postgres_execute_query(QUERY_HOURLY_PURCHARES)
    postgres_execute_query(QUERY_TOP10_SOURCES)


materialized_view_operation = PythonOperator(
    task_id='create_materialized_view_task',
    python_callable=create_materialized_view,
    provide_context=True,
    dag=dag
)

materialized_view_operation
