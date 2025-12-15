{{ config(materialized='table') }}

SELECT
    city,
    ROUND(AVG(price_m2), 2) AS avg_price_m2,
    ROUND(AVG(surface_m2), 2) AS avg_surface_m2,
    COUNT(*) AS nb_annonces
FROM {{ ref('stg_paruvendu') }}
GROUP BY city
ORDER BY avg_price_m2 DESC;
