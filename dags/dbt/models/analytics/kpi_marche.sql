{{ config(materialized='table') }}

SELECT
    MAX(date_scraping) AS last_scraping,
    COUNT(*) AS total_annonces,
    ROUND(AVG(price_m2), 2) AS avg_price_m2,
    ROUND(MIN(price_m2), 2) AS min_price_m2,
    ROUND(MAX(price_m2), 2) AS max_price_m2,
    ROUND(AVG(surface_m2), 2) AS avg_surface,
    ROUND(AVG(price_eur), 2) AS avg_rent
FROM {{ ref('stg_paruvendu') }};
