def create_stg_browser_events_table():
    return """
            CREATE TABLE IF NOT EXISTS public.stg_browser_events
            (
              click_id           VARCHAR(40),
              event_id           VARCHAR(40),
              event_timestamp    TIMESTAMP,
              event_type         VARCHAR(10),
              browser_name       VARCHAR(20),
              browser_language   VARCHAR(10),
              browser_user_agent TEXT
            );
            """


def staging_update_query(execution_date):
    f"""
    BEGIN;

    DELETE FROM public.stg_browser_events
    WHERE event_timestamp >= '{execution_date}'
        AND event_timestamp < CAST('{execution_date}' AS timestamp) + INTERVAL '1' HOUR;

    WITH
      data_truncate AS (
        SELECT
          click_id,
          event_id,
          event_timestamp::timestamp AS event_timestamp,
          event_type,
          browser_name,
          browser_language,
          browser_user_agent,
          row_number() OVER (PARTITION BY click_id, event_id, event_timestamp,
            event_type, browser_name, browser_language, browser_user_agent) AS rn
        FROM public.browser_events
        WHERE date_trunc('hour', event_timestamp::timestamp) = '{execution_date}')

    SELECT
      click_id,
      event_id,
      event_timestamp,
      event_type,
      browser_name,
      browser_language,
      browser_user_agent
    FROM data_truncate
    WHERE rn = 1
    ;

    COMMIT;
"""
