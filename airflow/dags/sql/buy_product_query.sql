-- Исходим из предположения, что товар, положенный в cart там остается
-- и покупается, если в эту click_id пользователь совершил /confirmation

--BEGIN;
--
--DELETE FROM dm_buy_product_table
--WHERE event_timestamp >= '{{ date_hour }}'
--    AND event_timestamp < '{{ date_hour }}' + INTERVAL 1 HOUR;

INSERT INTO dm_buy_product_table
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
    WHERE date_trunc('hour', CAST(event_timestamp AS timestamp)) = '{{ date_hour }}'  -- for jinja {{ date_hour }}
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
  event_timestamp
FROM product_buy_chain
WHERE page_url_path LIKE '/product_%'
  AND has_confirmation = 1
;

--COMMIT;