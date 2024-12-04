def create_dist_events_table():
    return """
            CREATE TABLE IF NOT EXISTS public.dm_dist_events (
              event_timestamp TIMESTAMP NOT NULL,
              count_cart INTEGER ,
              count_payment INTEGER,
              count_confirmation INTEGER,
              primary key(event_timestamp)
            );
            """
 
 
def query_dist_events(execution_date):
    # query Распределение событий по часам (столбики)
    return f"""
        INSERT INTO public.dm_dist_events (
            event_timestamp,
            count_cart,
            count_payment,
            count_confirmation
        )
        with cart as (
        select
            event_timestamp,
            count_cart
        from (
        select
            date_trunc('hour', event_timestamp) as event_timestamp,
            count(page_url_path) as count_cart
        from public.core_buy_product_table
        where date_trunc('hour', cast(event_timestamp AS timestamp)) = '{execution_date}'
        group by date_trunc('hour', event_timestamp)
        )),
        pc as (
        select
            event_timestamp,
            count_payment,
            count_confirmation
        from public.core_payment_confirm
        where event_timestamp = '{execution_date}'
        )
        select
            cart.event_timestamp,
            count_cart,
            count_payment,
            count_confirmation
        from cart
        left join pc on cart.event_timestamp=pc.event_timestamp
           
        ON CONFLICT (event_timestamp) DO UPDATE
            SET count_cart = excluded.count_cart,
                count_payment = excluded.count_payment,
                count_confirmation = excluded.count_confirmation
        """
