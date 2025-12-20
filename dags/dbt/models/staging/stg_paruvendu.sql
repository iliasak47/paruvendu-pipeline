{{ config(materialized='view') }}

SELECT
    CAST(id_annonce AS VARCHAR) AS id_annonce,
    UPPER(SUBSTR(title, 1, 1)) || LOWER(SUBSTR(title, 2)) AS title,
    CAST(price_eur AS DOUBLE) AS price_eur,
    CAST(surface_m2 AS DOUBLE) AS surface_m2,
    CAST(nb_pieces AS INTEGER) AS nb_pieces,
    ROUND(price_eur / NULLIF(surface_m2, 0), 2) AS price_m2,
    UPPER(SUBSTR(city, 1, 1)) || LOWER(SUBSTR(city, 2)) AS city,
    TRIM(description) AS description,
    date_add('day', CAST(date_scraping AS INTEGER), DATE '1970-01-01') AS date_scraping
FROM {{ source('paruvendu_history_source', 'paruvendu_history') }}
WHERE price_eur IS NOT NULL
  AND surface_m2 > 0;



