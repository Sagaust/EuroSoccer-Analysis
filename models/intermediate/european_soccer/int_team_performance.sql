-- models/intermediate/european_soccer/int_team_performance.sql

{{ config(materialized='view') }}

WITH team_matches AS (
    SELECT
        m.season,
        CASE WHEN m.home_team_api_id = t.team_api_id THEN 'home' ELSE 'away' END AS home_away,
        m.home_team_api_id,
        m.away_team_api_id,
        m.home_team_goal,
        m.away_team_goal,
        t.team_api_id,
        t.team_long_name
    FROM {{ ref('stg_matches') }} m
    JOIN {{ ref('stg_team') }} t ON m.home_team_api_id = t.team_api_id OR m.away_team_api_id = t.team_api_id
),

team_goals AS (
    SELECT
        team_api_id,
        season,
        SUM(CASE WHEN home_away = 'home' THEN home_team_goal ELSE away_team_goal END) AS goals_for,
        SUM(CASE WHEN home_away = 'home' THEN away_team_goal ELSE home_team_goal END) AS goals_against,
        COUNT(*) AS total_matches
    FROM team_matches
    GROUP BY team_api_id, season
),

team_attributes AS (
    SELECT
        team_id,
        AVG(buildUpPlaySpeed) AS avg_build_up_play_speed,
        AVG(buildUpPlayDribbling) AS avg_build_up_play_dribbling,
        AVG(buildUpPlayPassing) AS avg_build_up_play_passing,
        AVG(chanceCreationPassing) AS avg_chance_creation_passing,
        AVG(chanceCreationCrossing) AS avg_chance_creation_crossing,
        AVG(chanceCreationShooting) AS avg_chance_creation_shooting,
        AVG(defencePressure) AS avg_defence_pressure,
        AVG(defenceAggression) AS avg_defence_aggression,
        AVG(defenceTeamWidth) AS avg_defence_team_width,
        MAX(attribute_date) AS last_attribute_update  -- Latest date of team attribute record
    FROM {{ ref('stg_team_attributes') }}
    GROUP BY team_id
)

SELECT
    tm.team_api_id,
    tm.team_long_name,
    tm.season,
    tg.goals_for,
    tg.goals_against,
    tg.total_matches,
    ta.avg_build_up_play_speed,
    ta.avg_build_up_play_dribbling,
    ta.avg_build_up_play_passing,
    ta.avg_chance_creation_passing,
    ta.avg_chance_creation_crossing,
    ta.avg_chance_creation_shooting,
    ta.avg_defence_pressure,
    ta.avg_defence_aggression,
    ta.avg_defence_team_width,
    ta.last_attribute_update
FROM team_matches tm
JOIN team_goals tg ON tm.team_api_id = tg.team_api_id AND tm.season = tg.season
JOIN team_attributes ta ON tm.team_api_id = ta.team_id  -- Adjusted join condition to match team_id
GROUP BY tm.team_api_id, tm.team_long_name, tm.season, tg.goals_for, tg.goals_against, tg.total_matches, ta.avg_build_up_play_speed, ta.avg_build_up_play_dribbling, ta.avg_build_up_play_passing, ta.avg_chance_creation_passing, ta.avg_chance_creation_crossing, ta.avg_chance_creation_shooting, ta.avg_defence_pressure, ta.avg_defence_aggression, ta.avg_defence_team_width, ta.last_attribute_update
