{{
  config(
    materialized='incremental',
    unique_key=['timeframe', 'candle_start'],
    incremental_strategy='merge',
    merge_update_columns=[
      'open_price', 'high_price', 'low_price', 'close_price',
      'ticks_5m_count', 'price_diff', 'sma_20', 'sma_50',
      'high_fractal_3', 'high_fractal_5',
      'low_fractal_3', 'low_fractal_5',
      'is_complete_5',
      'dbt_updated_at'
    ],
    partition_by={
      'field': 'candle_start',
      'data_type': 'datetime',
      'granularity': 'day'
    },
    cluster_by=['timeframe']
  )
}}

with base as (
    -- Selecting source columns from staging
    select 
        observed_at, 
        open_price, 
        high_price, 
        low_price, 
        close_price 
    from {{ ref('stg_eurusd') }}

    {% if is_incremental() %}
    -- Even with daily loads, we look back to provide enough rows for SMA calculation.
    -- For 24h timeframe, SMA 50 needs at least 50 previous days.
    where observed_at >= (select datetime_sub(max(candle_start), interval 60 day) from {{ this }})
    {% endif %}
),

shifted as (
    -- Shift for NY Close consistency (GMT+2) for intraday timeframes
    select 
        *, 
        datetime_add(observed_at, interval 2 HOUR) as observed_at_shifted
    from base
),

timeframes as (
    -- 5m to 12h (With NY Shift)
    select '5m' as timeframe, datetime_sub(datetime_add(datetime_trunc(observed_at_shifted, HOUR), interval div(extract(minute from observed_at_shifted), 5) * 5 MINUTE), interval 2 HOUR) as candle_start, observed_at, open_price, high_price, low_price, close_price from shifted
    union all
    select '15m' as timeframe, datetime_sub(datetime_add(datetime_trunc(observed_at_shifted, HOUR), interval div(extract(minute from observed_at_shifted), 15) * 15 MINUTE), interval 2 HOUR) as candle_start, observed_at, open_price, high_price, low_price, close_price from shifted
    union all
    select '30m' as timeframe, datetime_sub(datetime_add(datetime_trunc(observed_at_shifted, HOUR), interval div(extract(minute from observed_at_shifted), 30) * 30 MINUTE), interval 2 HOUR) as candle_start, observed_at, open_price, high_price, low_price, close_price from shifted
    union all
    select '1h' as timeframe, datetime_sub(datetime_trunc(observed_at_shifted, HOUR), interval 2 HOUR) as candle_start, observed_at, open_price, high_price, low_price, close_price from shifted
    union all
    select '4h' as timeframe, datetime_sub(datetime_add(datetime_trunc(observed_at_shifted, DAY), interval div(extract(hour from observed_at_shifted), 4) * 4 HOUR), interval 2 HOUR) as candle_start, observed_at, open_price, high_price, low_price, close_price from shifted
    union all
    select '12h' as timeframe, datetime_sub(datetime_add(datetime_trunc(observed_at_shifted, DAY), interval div(extract(hour from observed_at_shifted), 12) * 12 HOUR), interval 2 HOUR) as candle_start, observed_at, open_price, high_price, low_price, close_price from shifted
    
    union all
    -- 24h (Pure UTC - No Shift)
    select '24h' as timeframe, datetime_trunc(observed_at, DAY) as candle_start, observed_at, open_price, high_price, low_price, close_price from base
),

aggregated as (
    select
        timeframe,
        candle_start,
        array_agg(open_price order by observed_at asc limit 1)[offset(0)] as open_price,
        max(high_price) as high_price,
        min(low_price) as low_price,
        array_agg(close_price order by observed_at desc limit 1)[offset(0)] as close_price,
        -- Updated name as your source data is in 5-minute ticks
        count(*) as ticks_5m_count
    from timeframes
    group by 1, 2
),

indicators as (
    select
        *,
        close_price - lag(close_price) over (partition by timeframe order by candle_start) as price_diff,
        avg(close_price) over (partition by timeframe order by candle_start rows between 19 preceding and current row) as sma_20,
        avg(close_price) over (partition by timeframe order by candle_start rows between 49 preceding and current row) as sma_50
    from aggregated
),

with_fractals as (
    select
        *,
        -- Williams fractals: lag/lead window per timeframe ordered by candle_start.
        -- ±1 neighbours (3-bar definition):
        lag(high_price, 1)  over w as h_p1,
        lead(high_price, 1) over w as h_n1,
        lag(low_price, 1)   over w as l_p1,
        lead(low_price, 1)  over w as l_n1,
        -- ±2 neighbours (extra constraint for 5-bar definition):
        lag(high_price, 2)  over w as h_p2,
        lead(high_price, 2) over w as h_n2,
        lag(low_price, 2)   over w as l_p2,
        lead(low_price, 2)  over w as l_n2
    from indicators
    window w as (partition by timeframe order by candle_start)
)

select
    timeframe,
    candle_start,
    open_price,
    high_price,
    low_price,
    close_price,
    ticks_5m_count,
    price_diff,
    sma_20,
    sma_50,

    -- 3-bar Williams up-fractal: high(N) > both immediate neighbours
    coalesce(
        high_price > h_p1
        and high_price > h_n1,
        false
    ) as high_fractal_3,

    -- 5-bar Williams up-fractal: high(N) > ±1 and ±2 neighbours (subset of 3-bar)
    coalesce(
        high_price > h_p1
        and high_price > h_n1
        and high_price > h_p2
        and high_price > h_n2,
        false
    ) as high_fractal_5,

    -- 3-bar Williams down-fractal
    coalesce(
        low_price < l_p1
        and low_price < l_n1,
        false
    ) as low_fractal_3,

    -- 5-bar Williams down-fractal
    coalesce(
        low_price < l_p1
        and low_price < l_n1
        and low_price < l_p2
        and low_price < l_n2,
        false
    ) as low_fractal_5,

    -- TRUE when bar has full ±2 neighbours, so 5-bar verdict is final.
    -- FALSE for the latest 2 bars per timeframe (right-side window not yet known).
    (h_p2 is not null and h_n2 is not null
     and l_p2 is not null and l_n2 is not null) as is_complete_5,

    current_timestamp() as dbt_inserted_at,
    current_timestamp() as dbt_updated_at

from with_fractals