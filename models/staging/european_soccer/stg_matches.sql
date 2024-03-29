WITH source_data AS (
  SELECT
    match_api_id,
    season,
    date AS game_date,
    home_team_api_id,
    league_id,
    home_player_1, 
    home_player_2, 
    home_player_3, 
    home_player_4, 
    home_player_5, 
    home_player_6, 
    home_player_7, 
    home_player_8, 
    home_player_9, 
    home_player_10, 
    home_player_11,
    away_team_api_id, 
    away_player_1, 
    away_player_2, 
    away_player_3, 
    away_player_4, 
    away_player_5, 
    away_player_6, 
    away_player_7, 
    away_player_8, 
    away_player_9, 
    away_player_10, 
    away_player_11,
    home_team_goal,
    away_team_goal,
    
  FROM
    {{ source('European_Soccer', 'Match') }}
)

SELECT
  *
FROM
  source_data
