-- models/my_new_project/staging/european_soccer/stg_league.sql

WITH source AS (
    SELECT
        id,
        country_id,
        name AS league_name
    FROM {{ source('European_Soccer', 'League') }}
)

SELECT
    id AS league_id,
    country_id,
    league_name
FROM source
