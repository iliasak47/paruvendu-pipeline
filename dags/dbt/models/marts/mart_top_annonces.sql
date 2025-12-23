-- depends_on: {{ ref('stg_paruvendu') }}

{{ config(materialized='view') }}

WITH stats AS (
    SELECT
        approx_percentile(price_m2, 0.2) AS p20
    FROM {{ ref('stg_paruvendu') }}
)

SELECT
    s.id_annonce,
    s.title,
    s.city,
    s.price_eur,
    s.surface_m2,
    s.price_m2,
    s.date_scraping,
    s.description
FROM {{ ref('stg_paruvendu') }} AS s
CROSS JOIN stats
WHERE s.price_m2 <= stats.p20
ORDER BY s.price_m2 ASC
LIMIT 20;
