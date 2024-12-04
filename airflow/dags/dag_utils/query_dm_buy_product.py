def create_buy_product_table():
    return """
            CREATE TABLE IF NOT EXISTS public.dm_buy_product (
              event_timestamp TIMESTAMP NOT NULL,
              count_buy_product INTEGER NOT NULL,
              primary key(event_timestamp)
            );
            """
 
 
def buy_product_query(execution_date):
    # query Количество купленных товаров в разрезе часа (либо таблица, либо пироги)
    return f"""
        INSERT INTO public.dm_buy_product (
            event_timestamp,
            count_buy_product
        )
        select
            event_timestamp,
            count_buy_product
        from (
        select
            date_trunc('hour', event_timestamp) as event_timestamp,
            count(page_url_path) as count_buy_product
        from public.core_buy_product_table
        where date_trunc('hour', cast(event_timestamp AS timestamp)) = '{execution_date}'
        group by date_trunc('hour', event_timestamp)
        ) bp
        
        ON CONFLICT (event_timestamp) DO UPDATE
            SET count_buy_product = excluded.count_buy_product   
        """