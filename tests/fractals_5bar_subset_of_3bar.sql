-- Invariant: a 5-bar fractal is by definition also a 3-bar fractal,
-- because high(N) > high(N±1) is part of both definitions.
-- Any row where 5-bar is TRUE but 3-bar is FALSE indicates a logic bug.
-- Test passes when the query returns 0 rows.

select
    timeframe,
    candle_start,
    high_fractal_3,
    high_fractal_5,
    low_fractal_3,
    low_fractal_5
from {{ ref('fct_eurusd_timeframes') }}
where (high_fractal_5 and not high_fractal_3)
   or (low_fractal_5 and not low_fractal_3)
