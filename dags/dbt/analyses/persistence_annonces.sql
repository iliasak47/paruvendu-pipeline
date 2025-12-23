-- Analyse de la persistance des annonces dans le temps
-- Objectif : identifier les annonces qui restent visibles longtemps
-- et qui influencent fortement les indicateurs historiques

WITH presence AS (
    SELECT
        id_annonce,
        MIN(date_scraping) AS first_seen,
        MAX(date_scraping) AS last_seen,
        COUNT(DISTINCT date_scraping) AS nb_weeks_visible,
        AVG(price_m2) AS avg_price_m2,
        AVG(price_eur) AS avg_rent,
        AVG(surface_m2) AS avg_surface
    FROM {{ ref('stg_paruvendu') }}
    GROUP BY id_annonce
)

SELECT
    id_annonce,
    first_seen,
    last_seen,
    nb_weeks_visible,
    ROUND(avg_price_m2, 2) AS avg_price_m2,
    ROUND(avg_rent, 2) AS avg_rent,
    ROUND(avg_surface, 2) AS avg_surface
FROM presence
ORDER BY nb_weeks_visible DESC
LIMIT 20;
