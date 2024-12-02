def create_raw_browser_events_table():
    return """
            CREATE TABLE IF NOT EXISTS public.raw_browser_events
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


def create_raw_device_events_table():
    return """
            CREATE TABLE IF NOT EXISTS public.raw_device_events
            (
              click_id           VARCHAR(40),
              device_type        VARCHAR(20),
              device_is_mobile   BOOLEAN,
              user_custom_id     VARCHAR(50),
              user_domain_id     VARCHAR(40),
              os                 TEXT,
              os_name            VARCHAR(20),
              os_timezone        VARCHAR(30),
              data_partition_ts  TIMESTAMP
            );
            """


def create_raw_geo_events_table():
    return """
            CREATE TABLE IF NOT EXISTS public.raw_geo_events
            (
              click_id          VARCHAR(40),
              geo_country       VARCHAR(10),
              geo_timezone      VARCHAR(30),
              geo_region_name   VARCHAR(40),
              ip_address        VARCHAR(30),
              geo_latitude      DOUBLE PRECISION,
              geo_longitude     DOUBLE PRECISION,
              data_partition_ts TIMESTAMP
            );
            """


def create_raw_location_events_table():
    return """
            CREATE TABLE IF NOT EXISTS public.raw_location_events
            (
              event_id          VARCHAR(40),
              page_url          TEXT,
              page_url_path     TEXT,
              referer_url       TEXT,
              referer_medium    VARCHAR(32),
              utm_medium        VARCHAR(32),
              utm_source        VARCHAR(32),
              utm_content       VARCHAR(32),
              utm_campaign      VARCHAR(32),
              data_partition_ts TIMESTAMP WITH TIME ZONE
            );
            """


def raw_browser_events_update_query(execution_date):
    return f"""
            BEGIN;
        
            DELETE FROM public.raw_browser_events
            WHERE event_timestamp >= '{execution_date}'
                AND event_timestamp < CAST('{execution_date}' AS timestamp) + INTERVAL '1' HOUR;
                
            INSERT INTO public.raw_browser_events
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


def raw_device_events_update_query(execution_date):
    return f"""
            BEGIN;

            DELETE FROM public.raw_device_events
            WHERE data_partition_ts >= '{execution_date}'
              AND data_partition_ts < CAST('{execution_date}' AS timestamp) + INTERVAL '1' HOUR;

            INSERT INTO public.raw_device_events
            WITH
              data_truncate AS (
                SELECT
                  click_id,
                  device_type,
                  device_is_mobile,
                  user_custom_id,
                  user_domain_id,
                  os,
                  os_name,
                  os_timezone,
                  data_partition_ts,
                  row_number() OVER (PARTITION BY click_id, device_type, device_is_mobile,
                    user_custom_id, user_domain_id, os, os_name, os_timezone, data_partition_ts) AS rn
                FROM public.device_events
                WHERE date_trunc('hour', data_partition_ts::timestamp) = '{execution_date}')
            
            SELECT
              click_id,
              device_type,
              device_is_mobile,
              user_custom_id,
              user_domain_id,
              os,
              os_name,
              os_timezone,
              data_partition_ts
            FROM data_truncate
            WHERE rn = 1
            ;

            COMMIT;
"""


def raw_geo_events_update_query(execution_date):
    return f"""
            BEGIN;

            DELETE FROM public.raw_geo_events
            WHERE data_partition_ts >= '{execution_date}'
              AND data_partition_ts < CAST('{execution_date}' AS timestamp) + INTERVAL '1' HOUR;


            INSERT INTO public.raw_geo_events
            WITH
              data_truncate AS (
                SELECT
                  click_id,
                  geo_country,
                  geo_timezone,
                  geo_region_name,
                  ip_address,
                  geo_latitude,
                  geo_longitude,
                  data_partition_ts,
                  row_number() OVER (PARTITION BY click_id, geo_country, geo_timezone,
                    geo_region_name, ip_address, geo_latitude, geo_longitude, data_partition_ts) AS rn
                FROM public.geo_events
                WHERE date_trunc('hour', data_partition_ts::timestamp) = '{execution_date}')
            
            SELECT
              click_id,
              geo_country,
              geo_timezone,
              geo_region_name,
              ip_address,
              geo_latitude,
              geo_longitude,
              data_partition_ts
            FROM data_truncate
            WHERE rn = 1
            ;

            COMMIT;
"""


def raw_location_events_update_query(execution_date):
    return f"""
            BEGIN;

             DELETE FROM public.raw_location_events
             WHERE data_partition_ts >= '{execution_date}'
               AND data_partition_ts < CAST('{execution_date}' AS timestamp) + INTERVAL '1' HOUR;
            
            
            INSERT INTO public.raw_location_events
            WITH
              data_truncate AS (
                SELECT
                  event_id,
                  page_url,
                  page_url_path,
                  referer_url,
                  referer_medium,
                  utm_medium,
                  utm_source,
                  utm_content,
                  utm_campaign,
                  data_partition_ts,
                  row_number() OVER (PARTITION BY event_id, page_url, page_url_path, referer_url,
                    referer_medium, utm_medium, utm_source, utm_content, utm_campaign, data_partition_ts) AS rn
                FROM public.location_events
                WHERE date_trunc('hour', data_partition_ts::timestamp) = '{execution_date}')
            
            SELECT
              event_id,
              page_url,
              page_url_path,
              referer_url,
              referer_medium,
              utm_medium,
              utm_source,
              utm_content,
              utm_campaign,
              data_partition_ts
            FROM data_truncate
            WHERE rn = 1
            ;

            COMMIT;
"""
