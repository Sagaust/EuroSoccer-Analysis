WITH home_team AS(
SELECT
league_id, season, match_id, team_id, home_team_goal AS goal, 'Home' AS category, CONCAT(home_team_goal, '-',away_team_goal) AS score
FROM {{ ref('stg_match') }}
UNPIVOT(team_id FOR metric IN (home_team_id))
),

away_team AS(
SELECT
league_id, season, match_id, team_id, away_team_goal AS goal, 'Away' AS category, CONCAT(home_team_goal, '-',away_team_goal) AS score
FROM {{ ref('stg_match') }}
UNPIVOT(team_id FOR metric IN (away_team_id))
),

calculate_final_score AS (
SELECT
  match_id,
  CASE
    WHEN home_team_goal > away_team_goal THEN CAST(home_team_id AS STRING)
    WHEN home_team_goal < away_team_goal THEN CAST(away_team_id AS STRING)
    ELSE 'Draw'
  END AS victorious_team_id
FROM {{ ref('stg_match') }}

),

union_team AS (
SELECT
*
FROM home_team
UNION ALL
SELECT
*
FROM away_team
)

SELECT
  team.*,
  score.victorious_team_id
FROM union_team AS team
LEFT JOIN calculate_final_score AS score USING (match_id)
ORDER BY season, match_id, category DESC