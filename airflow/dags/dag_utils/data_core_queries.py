def create_buy_product_table():
    return """
            CREATE TABLE IF NOT EXISTS public.core_buy_product_table (
              click_id VARCHAR(40) NOT NULL,
              user_custom_id VARCHAR(50) NOT NULL,
              page_url_path TEXT NOT NULL,
              event_timestamp TIMESTAMP NOT NULL
            );
            """


def buy_product_query(execution_date):
    # query Покупка товаров
    return f"""
        BEGIN;
        
        DELETE FROM public.core_buy_product_table
        WHERE event_timestamp >= '{execution_date}'
            AND event_timestamp < CAST('{execution_date}' AS timestamp) + INTERVAL '1' HOUR;
            
        INSERT INTO public.core_buy_product_table
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
            FROM public.raw_location_events l
            LEFT OUTER JOIN public.raw_browser_events be ON l.event_id = be.event_id
            LEFT OUTER JOIN public.raw_device_events de ON be.click_id = de.click_id
            WHERE date_trunc('hour', CAST(event_timestamp AS timestamp)) = '{execution_date}'  -- for jinja
            ORDER BY event_timestamp),
            
          next_url_path_data AS (
            SELECT
              event_id,
              click_id,
              user_custom_id,
              page_url_path,
              -- to see product was added to cart
              LEAD(page_url_path) OVER (PARTITION BY click_id, user_custom_id ORDER BY event_timestamp) AS next_url_path,
              event_timestamp
            FROM merged_data
            WHERE rn = 1),
            
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
