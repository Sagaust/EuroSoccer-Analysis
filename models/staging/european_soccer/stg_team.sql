-- models/my_new_project/staging/european_soccer/stg_team.sql

WITH source AS (
    SELECT
        team_api_id AS team_id,
        team_fifa_api_id,
        team_long_name,
        team_short_name
    FROM {{ source('European_Soccer', 'Team') }}
)

SELECT
    team_id,
    team_fifa_api_id,
    team_long_name,
    team_short_name
FROM source
