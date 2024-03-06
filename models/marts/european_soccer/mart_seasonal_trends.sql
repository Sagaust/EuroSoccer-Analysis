-- mart_seasonal_trends.sql
-- Description: Analyzes trends over seasons, including changes in team and league performance, and player performance improvements.

{{ config(materialized='table') }}

WITH league_trends AS (
    SELECT
        CAST(season AS STRING) AS season,  -- Ensuring season is STRING type for compatibility
        league_name,
        AVG(total_goals) AS avg_goals_per_season,
        AVG(total_matches) AS avg_matches_per_season,
        AVG(home_wins + away_wins) AS avg_wins_per_season,
        AVG(draws) AS avg_draws_per_season
    FROM {{ ref('int_league_analytics') }}
    GROUP BY season, league_name
),

team_trends AS (
    SELECT
        CAST(season AS STRING) AS season,
        team_long_name,
        AVG(goals_for) AS avg_goals_for_per_season,
        AVG(goals_against) AS avg_goals_against_per_season,
        CAST(COUNT(*) AS FLOAT64) AS total_matches  -- Ensuring total_matches is FLOAT64 for compatibility
    FROM {{ ref('int_team_performance') }}
    GROUP BY season, team_long_name
),

player_trends AS (
    SELECT
        CAST(season_year AS STRING) AS season,
        player_name,
        AVG(avg_overall_rating) AS avg_rating_per_season,
        AVG(avg_potential) AS avg_potential_per_season,
        CAST(NULL AS FLOAT64) AS total_matches  -- Placeholder for total_matches to ensure compatibility
    FROM {{ ref('int_player_performance') }}
    GROUP BY season_year, player_name
)

SELECT
    'League Trends' AS trend_type,
    season,
    league_name AS name,
    CAST(avg_goals_per_season AS FLOAT64) AS avg_goals_per_season,
    avg_matches_per_season,
    avg_wins_per_season,
    avg_draws_per_season,
    CAST(NULL AS STRING) AS team_long_name,
    CAST(NULL AS STRING) AS player_name,
    CAST(NULL AS FLOAT64) AS avg_rating_per_season,
    CAST(NULL AS FLOAT64) AS avg_potential_per_season
FROM league_trends

UNION ALL

SELECT
    'Team Trends' AS trend_type,
    season,
    CAST(NULL AS STRING) AS name,
    CAST(NULL AS FLOAT64) AS avg_goals_per_season,
    total_matches,
    CAST(NULL AS FLOAT64) AS avg_wins_per_season,
    CAST(NULL AS FLOAT64) AS avg_draws_per_season,
    team_long_name,
    CAST(NULL AS STRING) AS player_name,
    CAST(NULL AS FLOAT64) AS avg_rating_per_season,
    CAST(NULL AS FLOAT64) AS avg_potential_per_season
FROM team_trends

UNION ALL

SELECT
    'Player Trends' AS trend_type,
    season,
    CAST(NULL AS STRING) AS name,
    CAST(NULL AS FLOAT64) AS avg_goals_per_season,
    total_matches,  -- Placeholder value to maintain column alignment
    CAST(NULL AS FLOAT64) AS avg_wins_per_season,
    CAST(NULL AS FLOAT64) AS avg_draws_per_season,
    CAST(NULL AS STRING) AS team_long_name,
    player_name,
    avg_rating_per_season,
    avg_potential_per_season
FROM player_trends

ORDER BY trend_type, season, name, team_long_name, player_name
