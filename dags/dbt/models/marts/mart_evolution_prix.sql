{{ config(materialized='table') }}

SELECT
    date_scraping,
    ROUND(AVG(price_m2), 2) AS avg_price_m2,
    ROUND(AVG(price_eur), 2) AS avg_price,
    COUNT(*) AS nb_annonces
FROM {{ ref('stg_paruvendu') }}
GROUP BY date_scraping
ORDER BY date_scraping;
