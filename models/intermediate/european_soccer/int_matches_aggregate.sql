{{ config(materialized='table') }} 

WITH home_player_team AS (
  SELECT
    home_team_api_id,
    match_api_id,
    home_player
  FROM {{ ref('stg_matches') }},
    UNNEST([home_player_1, home_player_2, home_player_3, home_player_4, home_player_5, home_player_6, home_player_7, home_player_8, home_player_9, home_player_10, home_player_11]) AS home_player
  GROUP BY
    home_team_api_id,
    match_api_id,
    home_player
),
away_player_team AS (
  SELECT
    away_team_api_id,
    match_api_id,
    away_player
  FROM {{ ref('stg_matches') }},
    UNNEST([away_player_1, away_player_2, away_player_3, away_player_4, away_player_5, away_player_6, away_player_7, away_player_8, away_player_9, away_player_10, away_player_11]) AS away_player
  GROUP BY
    away_team_api_id,
    match_api_id,
    away_player
)
SELECT DISTINCT
  match.season,
  match.home_team_api_id,
  home_player.home_player,
  player.player_name AS home_player_name,
  player2.player_name AS away_player_name
FROM {{ ref('stg_matches') }} as match
LEFT JOIN
  home_player_team AS home_player ON match.home_team_api_id = home_player.home_team_api_id
  AND match.match_api_id = home_player.match_api_id
LEFT JOIN
  away_player_team AS away_player ON match.away_team_api_id = away_player.away_team_api_id
  AND match.match_api_id = away_player.match_api_id
LEFT JOIN
  `austwagonproject.European_Soccer.stg_players` AS player ON home_player.home_player = player.player_api_id
LEFT JOIN
  {{ ref('stg_players') }} AS player2 ON away_player.away_player = player2.player_api_id
WHERE
  match.match_api_id = 508287
