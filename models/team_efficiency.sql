-- models/team_efficiency.sql
WITH team_performance AS (
  SELECT
    t.team_api_id,
    t.team_long_name,
    COUNT(m.match_api_id) AS total_matches,
    SUM(CASE WHEN m.home_team_api_id = t.team_api_id AND m.home_team_goal > m.away_team_goal THEN 1
             WHEN m.away_team_api_id = t.team_api_id AND m.away_team_goal > m.home_team_goal THEN 1
             ELSE 0 END) AS wins,
    SUM(CASE WHEN m.home_team_api_id = t.team_api_id AND m.home_team_goal = m.away_team_goal THEN 1
             WHEN m.away_team_api_id = t.team_api_id AND m.away_team_goal = m.home_team_goal THEN 1
             ELSE 0 END) AS draws,
    SUM(CASE WHEN m.home_team_api_id = t.team_api_id AND m.home_team_goal < m.away_team_goal THEN 1
             WHEN m.away_team_api_id = t.team_api_id AND m.away_team_goal < m.home_team_goal THEN 1
             ELSE 0 END) AS losses
  FROM
    `austwagonproject.European_Soccer.Team` t
  JOIN
    `austwagonproject.European_Soccer.Match` m
    ON t.team_api_id = m.home_team_api_id OR t.team_api_id = m.away_team_api_id
  GROUP BY
    t.team_api_id, t.team_long_name
)

SELECT
  *,
  (wins * 3 + draws) AS total_points,
  ROUND((wins * 3 + draws) / total_matches, 2) AS efficiency
FROM
  team_performance
