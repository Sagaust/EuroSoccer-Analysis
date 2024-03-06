
{{ config(materialized='view') }}

WITH enhanced_team_attributes AS (
    SELECT
        team_id,
        AVG(buildUpPlaySpeed) AS avg_build_up_play_speed,
        AVG(buildUpPlayDribbling) AS avg_build_up_play_dribbling,
        AVG(buildUpPlayPassing) AS avg_build_up_play_passing,
        AVG(defencePressure) AS avg_defence_pressure,
        AVG(defenceAggression) AS avg_defence_aggression,
        AVG(defenceTeamWidth) AS avg_defence_team_width,
        attribute_date
    FROM {{ ref('stg_team_attributes') }}
    GROUP BY team_id, attribute_date
),

match_details AS (
    SELECT
        m.match_api_id,
        m.season,
        m.game_date AS match_date,
        m.home_team_api_id,
        m.away_team_api_id,
        hta.avg_build_up_play_speed AS home_team_avg_build_up_play_speed,
        hta.avg_defence_pressure AS home_team_avg_defence_pressure,
        ata.avg_build_up_play_speed AS away_team_avg_build_up_play_speed,
        ata.avg_defence_pressure AS away_team_avg_defence_pressure,
        m.home_team_goal,
        m.away_team_goal
    FROM {{ ref('stg_matches') }} m
    LEFT JOIN enhanced_team_attributes hta 
        ON m.home_team_api_id = hta.team_id 
        AND CAST(m.game_date AS DATE) = hta.attribute_date
    LEFT JOIN enhanced_team_attributes ata 
        ON m.away_team_api_id = ata.team_id 
        AND CAST(m.game_date AS DATE) = ata.attribute_date
)

SELECT *
FROM match_details
