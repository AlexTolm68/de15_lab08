def create_payment_confirm_table():
    return """
            CREATE TABLE IF NOT EXISTS public.core_payment_confirm (
              event_timestamp TIMESTAMP NOT NULL,
              count_payment INTEGER,
              count_confirmation INTEGER,
              primary key(event_timestamp)
            );
            """
 
 
def query_payment_confirm(execution_date):
    # query Распределение событий по часам (столбики)
    return f"""
        INSERT INTO public.core_payment_confirm (
            event_timestamp,
            count_payment,
            count_confirmation
        )
        with t1 as (
        select distinct
            be.click_id ,
            be.event_id ,
            be.event_timestamp,
            de.user_custom_id ,
        --    ge.geo_country ,
            le.page_url_path
        from public.raw_browser_events be
        left join public.device_events de on be.click_id = de.click_id
        --left join public.geo_events ge on be.click_id = ge.click_id
        left join public.location_events le on be.event_id = le.event_id
        where
            date_trunc('hour', be.event_timestamp) = '{execution_date}'
        ),
        t2 as (
        select
            click_id ,
            --event_id ,
            event_timestamp ,
            --device_type ,
            user_custom_id ,
            page_url_path as page_path,
            lead(page_url_path) over (partition by user_custom_id order by event_timestamp) as lead_page_path
        from t1
        ),
        t3 as (
        select
            t2.*,
            case when substring(page_path, 2, 7)='product' and lead_page_path = '/cart' then 1
                when page_path='/cart' and lead_page_path='/payment' then 2
                when page_path='/payment' and lead_page_path='/confirmation' then 3
                else 0
                end as score
        from t2
        ),
        t4 as (
        select
            t3.*,
            lead(score) over (partition by user_custom_id order by event_timestamp) as lead_score
        from t3
        where score!=0
        ),
        t5 as (
        select
            t4.*,
            lead_score - score as diff,
            case when lead_page_path='/confirmation' and score=3 then 1
            else 0
            end check_conf
        from t4
        ),
        t6 as(
        select
            t5.*
        from t5
        where diff != 0 or check_conf=1
        ),
        t7 as (
        select
            t6.*,
            lag(page_path, 2) over (partition by user_custom_id order by event_timestamp) as lag_product
        from t6
        --where diff=1 or diff<=-2
        ),
        t8 as (
        select
         *
        from t7
        where
            1=1
            and lead_page_path = '/confirmation'
            )
        select
            date_trunc('hour', event_timestamp) as event_timestamp,
            count(*) as count_payment,
            count(*) as count_confirmation
        from t8
        group by date_trunc('hour', event_timestamp)
 
 
        ON CONFLICT (event_timestamp) DO UPDATE
            SET count_payment = excluded.count_payment,
                count_confirmation = excluded.count_confirmation
        """
