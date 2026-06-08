{{
  config(
    materialized='incremental',
    unique_key=['timeframe', 'created_at', 'direction'],
    incremental_strategy='merge',
    merge_update_columns=[
      'first_touch_at', 'fully_filled_at', 'status', 'dbt_updated_at'
    ],
    partition_by={
      'field': 'created_at',
      'data_type': 'datetime',
      'granularity': 'day'
    },
    cluster_by=['timeframe', 'direction']
  )
}}

with source_bars as (
    select
        timeframe,
        candle_start,
        high_price,
        low_price,
        lag(high_price, 1) over w as prev_high,
        lag(low_price, 1) over w as prev_low,
        lag(candle_start, 1) over w as prev_start,
        lead(high_price, 1) over w as next_high,
        lead(low_price, 1) over w as next_low,
        lead(candle_start, 1) over w as next_start
    from {{ ref('fct_eurusd_timeframes') }}

    {% if is_incremental() %}
    where candle_start >= (select datetime_sub(max(created_at), interval 7 day) from {{ this }})
    {% endif %}

    window w as (partition by timeframe order by candle_start)
),

fvg_identified as (
    select
        timeframe,
        candle_start as created_at,
        prev_start as candle_1_start,
        next_start as candle_3_start,
        'bullish' as direction,
        prev_high as fvg_low,
        next_low as fvg_high,
        (next_low - prev_high) * 10000 as fvg_size_pips,
        (next_low + prev_high) / 2 as fvg_midpoint
    from source_bars
    where prev_high < next_low
      and prev_high is not null and next_low is not null

    union all

    select
        timeframe,
        candle_start as created_at,
        prev_start as candle_1_start,
        next_start as candle_3_start,
        'bearish' as direction,
        next_high as fvg_low,
        prev_low as fvg_high,
        (prev_low - next_high) * 10000 as fvg_size_pips,
        (prev_low + next_high) / 2 as fvg_midpoint
    from source_bars
    where prev_low > next_high
      and prev_low is not null and next_high is not null
),

fill_scan as (
    select
        f.timeframe,
        f.created_at,
        f.direction,
        f.candle_1_start,
        f.candle_3_start,
        f.fvg_low,
        f.fvg_high,
        f.fvg_size_pips,
        f.fvg_midpoint,
        min(case
            when f.direction = 'bullish' and b.low_price <= f.fvg_high then b.candle_start
            when f.direction = 'bearish' and b.high_price >= f.fvg_low then b.candle_start
        end) as first_touch_at,
        min(case
            when f.direction = 'bullish' and b.low_price <= f.fvg_low then b.candle_start
            when f.direction = 'bearish' and b.high_price >= f.fvg_high then b.candle_start
        end) as fully_filled_at
    from fvg_identified f
    left join {{ ref('fct_eurusd_timeframes') }} b
        on b.timeframe = f.timeframe
        and b.candle_start > f.candle_3_start
        and b.candle_start <= datetime_add(f.created_at, interval 30 day)
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select
    timeframe,
    created_at,
    direction,
    candle_1_start,
    candle_3_start,
    fvg_low,
    fvg_high,
    fvg_size_pips,
    fvg_midpoint,
    first_touch_at,
    fully_filled_at,
    case
        when fully_filled_at is not null then 'fully_filled'
        when first_touch_at is not null then 'partially_filled'
        else 'open'
    end as status,
    current_timestamp() as dbt_inserted_at,
    current_timestamp() as dbt_updated_at
from fill_scan
