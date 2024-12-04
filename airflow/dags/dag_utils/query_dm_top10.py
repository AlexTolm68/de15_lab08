def create_top10_page_table():
    return """
            CREATE TABLE IF NOT EXISTS public.dm_top10_page (
              event_timestamp TIMESTAMP NOT NULL,
              page_url_path TEXT NOT NULL,
              count_page_url_path INTEGER NOT NULL,
              primary key(event_timestamp, page_url_path)
            );
            """
 
 
def query_top10_page(execution_date):
    # query Топ-10 посещённых страниц, с которых был переход в покупку — список ссылок с количеством покупок
    return f"""
        INSERT INTO public.dm_top10_page (
            event_timestamp,
            page_url_path,
            count_page_url_path
        )
        select
            event_timestamp,
            page_url_path,
            count_page_url_path
        from (
        select
            date_trunc('hour', event_timestamp) as event_timestamp,
            page_url_path,
            count(page_url_path) as count_page_url_path
        from public.core_buy_product_table
        where date_trunc('hour', cast(event_timestamp AS timestamp)) = '{execution_date}'
        group by date_trunc('hour', event_timestamp) , page_url_path
        order By count_page_url_path DESC limit 10
        ) top
        
        ON CONFLICT (event_timestamp, page_url_path) DO UPDATE
            SET count_page_url_path = excluded.count_page_url_path
        """
