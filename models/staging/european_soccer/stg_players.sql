-- models/my_new_project/staging/european_soccer/stg_players.sql

WITH source AS (
    SELECT
        player_api_id,
        player_name,
        birthday,
        height,
        weight
    FROM {{ source('European_Soccer', 'Player') }}
)

SELECT
    player_api_id,
    player_name,
    birthday,
    height,
    weight,
    -- Correct calculation for age in BigQuery
    DATE_DIFF(CURRENT_DATE(), CAST(birthday AS DATE), YEAR) AS age
FROM source
