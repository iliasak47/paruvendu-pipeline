-- depends_on: {{ ref('stg_paruvendu') }}

{{ config(materialized='table') }}

SELECT
    nb_pieces,
    COUNT(*) AS nb_annonces,
    ROUND(AVG(price_m2), 2) AS avg_price_m2,
    ROUND(AVG(price_eur), 2) AS avg_rent,
    ROUND(AVG(surface_m2), 2) AS avg_surface
FROM {{ ref('stg_paruvendu') }}
GROUP BY nb_pieces
ORDER BY nb_pieces;
