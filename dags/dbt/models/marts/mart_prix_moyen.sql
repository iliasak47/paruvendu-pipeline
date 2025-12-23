-- depends_on: {{ ref('stg_paruvendu') }}

{{ config(materialized='table') }}

WITH daily_avg AS (
    SELECT
        city,
        date_scraping,
        AVG(price_m2) AS avg_price_m2_daily,
        AVG(surface_m2) AS avg_surface_m2_daily
    FROM {{ ref('stg_paruvendu') }}
    GROUP BY city, date_scraping
)

SELECT
    city,
    ROUND(AVG(avg_price_m2_daily), 2) AS avg_price_m2,
    ROUND(AVG(avg_surface_m2_daily), 2) AS avg_surface_m2,
    COUNT(*) AS nb_weeks
FROM daily_avg
GROUP BY city
ORDER BY avg_price_m2 DESC;
