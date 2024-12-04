def create_hourly_events_table():
    return """
            CREATE TABLE IF NOT EXISTS public.dm_hourly_events (
              event_hour TIMESTAMP NOT NULL,
              event_count INTEGER NOT NULL
            );
            """
 
 
def hourly_events_query(execution_date):
    # query Количество событий в разрезе часа
    return f"""
        insert into public.dm_hourly_events (
            event_hour,
            event_count
        )
        select 
            date_trunc('hour', event_timestamp::timestamp) AS event_hour,
            count(*) AS event_count
        from browser_events
        where date_trunc('hour', cast(event_timestamp AS timestamp)) = '{execution_date}'
        group by (date_trunc('hour', event_timestamp::timestamp))
        order by (date_trunc('hour', event_timestamp::timestamp))
        ) he
        
        on conflict (event_timestamp) do update
            SET event_count = excluded.event_count   
        """
