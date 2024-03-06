-- Description: Summarizes team performance across various seasons, including win/loss records,
-- goals scored and conceded, and average team stats like build-up play speed and defense pressure.
-- This model also includes league standings per season.

{{ config(materialized='table') }}

WITH team_performance AS (
    SELECT
        tp.season,
        tp.team_api_id,
        tp.team_long_name,
        COUNT(*) AS total_matches,
        SUM(CASE WHEN tp.goals_for > tp.goals_against THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN tp.goals_for < tp.goals_against THEN 1 ELSE 0 END) AS losses,
        SUM(CASE WHEN tp.goals_for = tp.goals_against THEN 1 ELSE 0 END) AS draws,
        SUM(tp.goals_for) AS goals_scored,
        SUM(tp.goals_against) AS goals_conceded,
        AVG(tp.avg_build_up_play_speed) AS avg_build_up_play_speed,
        AVG(tp.avg_defence_pressure) AS avg_defence_pressure,
        AVG(tp.avg_defence_aggression) AS avg_defence_aggression,
        AVG(tp.avg_defence_team_width) AS avg_defence_team_width
    FROM {{ ref('int_team_performance') }} tp
    GROUP BY tp.season, tp.team_api_id, tp.team_long_name
)

SELECT
    p.season,
    p.team_api_id,
    p.team_long_name,
    p.total_matches,
    p.wins,
    p.losses,
    p.draws,
    p.goals_scored,
    p.goals_conceded,
    p.avg_build_up_play_speed,
    p.avg_defence_pressure,
    p.avg_defence_aggression,
    p.avg_defence_team_width
FROM team_performance p
ORDER BY p.season, p.team_long_name
