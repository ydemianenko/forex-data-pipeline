{{
  config(
    materialized='incremental',
    unique_key='observed_at',
    partition_by={
      "field": "observed_at",
      "data_type": "datetime",
      "granularity": "day"
    },
    incremental_strategy='merge'
  )
}}

with source as (
    -- Звертаємося до нашого Bronze рівня
    select * from {{ source('internal_data', 'eurusd_raw') }}
    
    {% if is_incremental() %}
      -- Фільтр для дозапису: беремо тільки ті хвилини, яких ще немає в Silver. Додаємо CAST для обох сторін порівняння
      where cast(datetime as datetime) > (select max(observed_at) from {{ this }})
    {% endif %}
),

final as (
    select
        -- Перетворюємо текст на таймстемп
        cast(datetime as datetime) as observed_at,
        cast(open as float64) as open_price,
        cast(high as float64) as high_price,
        cast(low as float64) as low_price,
        cast(close as float64) as close_price,
        -- Технічна колонка: коли саме dbt обробив цей рядок
        current_timestamp() as dbt_updated_at
    from source
    -- Видаляємо дублікати, якщо одна й та сама хвилина прийшла двічі
    qualify row_number() over (
        partition by datetime 
        order by datetime desc
    ) = 1
)

select * from final