-- models/intermediate/european_soccer/int_league_analytics.sql

{{ config(materialized='view') }}

WITH match_outcomes AS (
    SELECT
        m.league_id,
        m.season,  -- Including the season directly from stg_matches
        CASE 
            WHEN m.home_team_goal > m.away_team_goal THEN 'Home Win'
            WHEN m.home_team_goal < m.away_team_goal THEN 'Away Win'
            ELSE 'Draw'
        END AS outcome,
        m.home_team_goal,
        m.away_team_goal
    FROM {{ ref('stg_matches') }} m
),

league_performance AS (
    SELECT
        l.league_id,
        l.league_name,
        c.country_name,
        mo.season,  
        COUNT(*) AS total_matches,
        COUNT(CASE WHEN mo.outcome = 'Home Win' THEN 1 END) AS home_wins,
        COUNT(CASE WHEN mo.outcome = 'Away Win' THEN 1 END) AS away_wins,
        COUNT(CASE WHEN mo.outcome = 'Draw' THEN 1 END) AS draws,
        SUM(mo.home_team_goal) + SUM(mo.away_team_goal) AS total_goals,
        SUM(mo.home_team_goal) AS home_goals_scored,
        SUM(mo.away_team_goal) AS away_goals_scored,
        SUM(mo.away_team_goal) AS goals_conceded_at_home,
        SUM(mo.home_team_goal) AS goals_conceded_away
    FROM match_outcomes mo
    JOIN {{ ref('stg_league') }} l ON mo.league_id = l.league_id
    JOIN {{ ref('stg_country') }} c ON l.country_id = c.country_id
    GROUP BY l.league_id, l.league_name, c.country_name, mo.season
)

SELECT
    lp.league_id,
    lp.league_name,
    lp.country_name,
    lp.season,  
    lp.total_matches,
    lp.home_wins,
    lp.away_wins,
    lp.draws,
    lp.total_goals,
    lp.home_goals_scored,
    lp.away_goals_scored,
    lp.goals_conceded_at_home,
    lp.goals_conceded_away
FROM league_performance lp
ORDER BY lp.season, lp.league_name, lp.total_goals DESC
