-- Corrected version for int_player_transfer_analysis.sql

{{ config(materialized='view') }}

WITH player_appearances_home AS (
    SELECT
        m.season,
        m.game_date,
        m.home_team_api_id AS team_api_id,
        player_api_id
    FROM {{ ref('stg_matches') }} m,
    UNNEST([m.home_player_1, m.home_player_2, m.home_player_3, m.home_player_4, m.home_player_5,
            m.home_player_6, m.home_player_7, m.home_player_8, m.home_player_9, m.home_player_10, m.home_player_11]) AS player_api_id
    WHERE player_api_id IS NOT NULL
),

player_appearances_away AS (
    SELECT
        m.season,
        m.game_date,
        m.away_team_api_id AS team_api_id,
        player_api_id
    FROM {{ ref('stg_matches') }} m,
    UNNEST([m.away_player_1, m.away_player_2, m.away_player_3, m.away_player_4, m.away_player_5,
            m.away_player_6, m.away_player_7, m.away_player_8, m.away_player_9, m.away_player_10, m.away_player_11]) AS player_api_id
    WHERE player_api_id IS NOT NULL
),

combined_player_appearances AS (
    SELECT * FROM player_appearances_home
    UNION ALL
    SELECT * FROM player_appearances_away
),

distinct_player_appearances AS (
    SELECT
        player_api_id,
        team_api_id,
        season,
        MIN(game_date) AS first_appearance_date
    FROM combined_player_appearances
    GROUP BY player_api_id, team_api_id, season
),

transfer_attempts AS (
    SELECT
        pla.player_api_id,
        pla.season,
        pla.team_api_id,
        LAG(pla.team_api_id) OVER (PARTITION BY pla.player_api_id ORDER BY pla.first_appearance_date) AS previous_team,
        pla.first_appearance_date
    FROM distinct_player_appearances pla
),

player_transfers AS (
    SELECT
        ta.player_api_id,
        ta.season,
        ta.team_api_id AS new_team,
        ta.previous_team,
        ta.first_appearance_date
    FROM transfer_attempts ta
    WHERE ta.previous_team IS NOT NULL
      AND ta.previous_team != ta.team_api_id
)

SELECT *
FROM player_transfers
ORDER BY player_api_id, season, first_appearance_date
