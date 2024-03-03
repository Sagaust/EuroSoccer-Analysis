{{ config(materialized='table') }} 

WITH team_base AS (
    SELECT
    team_api_id, 
    team_long_name,
    team_short_name
    FROM {{ ref('stg_team') }}
),

team_match_stats AS (
     -- We aggregate match results here
     SELECT
     -- Match the join type based on where you want to calculate stats for
     CASE WHEN home_team_api_id = team_api_id THEN home_team_api_id ELSE away_team_api_id END AS team_id, -- Updated!
     COUNT(*) AS matches_played,
     SUM(CASE WHEN home_team_api_id = team_api_id THEN home_team_goal ELSE away_team_goal END) AS goals_scored,
     SUM(CASE WHEN home_team_api_id = team_api_id THEN away_team_goal ELSE home_team_goal END) AS goals_conceded,
     -- ... Add more aggregations here 
   FROM {{ ref('stg_matches') }}
   GROUP BY 1 -- Grouping by the calculated `team_id` column
)

SELECT
  tb.team_id, -- Updated!
  tb.team_long_name,
  tb.team_short_name,
  tms.matches_played,
  tms.goals_scored,
  tms.goals_conceded
  -- ... we can add more calculations if needed 
FROM team_base tb
JOIN team_match_stats tms ON tb.team_id = tms.team_id
