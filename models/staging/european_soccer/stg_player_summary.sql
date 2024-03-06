-- models/staging/european_soccer/stg_player_summary.sql

{{ config(materialized='view') }}

WITH source AS (
    SELECT
        player_api_id,
        player_name
    FROM {{ source('European_Soccer', 'Player_Summary') }}
)

SELECT
    player_api_id,
    player_name
FROM source
