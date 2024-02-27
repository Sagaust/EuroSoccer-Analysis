-- models/match_outcomes.sql
WITH match_details AS (
  SELECT
    m.match_api_id,
    m.date,
    m.home_team_api_id,
    m.away_team_api_id,
    m.home_team_goal,
    m.away_team_goal,
    CASE
      WHEN m.home_team_goal > m.away_team_goal THEN 'Home Win'
      WHEN m.home_team_goal < m.away_team_goal THEN 'Away Win'
      ELSE 'Draw'
    END AS outcome
  FROM
    `austwagonproject.European_Soccer.Match` m
)

SELECT
  *,
  CASE
    WHEN outcome = 'Home Win' THEN home_team_api_id
    WHEN outcome = 'Away Win' THEN away_team_api_id
    ELSE NULL
  END AS winner_team_api_id
FROM
  match_details
