WITH source_data AS (
  SELECT
    match_api_id,
    season,
    date,
    home_team_api_id,
    away_team_api_id,
    home_team_goal,
    away_team_goal
  FROM
    {{ source('European_Soccer', 'Match') }}
)

SELECT
  *
FROM
  source_data
