-- Asserts that (timeframe, candle_start) is a unique composite key
-- in fct_eurusd_timeframes. Test passes when the query returns 0 rows.

select
    timeframe,
    candle_start,
    count(*) as duplicate_count
from {{ ref('fct_eurusd_timeframes') }}
group by 1, 2
having count(*) > 1
