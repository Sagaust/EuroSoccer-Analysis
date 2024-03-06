-- Corrected mart_league_ranking.sql

{{ config(materialized='table') }}

WITH league_summary AS (
    SELECT
        league_name,
        country_name,
        season,
        SUM(total_matches) AS total_matches,
        SUM(home_wins) AS total_home_wins,
        SUM(away_wins) AS total_away_wins,
        SUM(draws) AS total_draws,
        SUM(total_goals) AS total_goals,
        -- Ensure calculations for avg_goals_conceded_at_home and avg_goals_conceded_away are correctly defined
        AVG(goals_conceded_at_home) AS avg_goals_conceded_at_home,  -- Assumed provided by int_league_analytics
        AVG(goals_conceded_away) AS avg_goals_conceded_away  -- Assumed provided by int_league_analytics
    FROM {{ ref('int_league_analytics') }}
    GROUP BY league_name, country_name, season
),

league_rankings AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY season ORDER BY total_goals DESC) AS rank_by_goals,
        RANK() OVER (PARTITION BY season ORDER BY (total_home_wins + total_away_wins) DESC) AS rank_by_wins
    FROM league_summary
)

SELECT
    season,
    league_name,
    country_name,
    total_matches,
    total_home_wins,
    total_away_wins,
    total_draws,
    total_goals,
    avg_goals_conceded_at_home,
    avg_goals_conceded_away,
    (avg_goals_conceded_at_home + avg_goals_conceded_away) / 2 AS avg_goals_conceded,
    rank_by_goals,
    rank_by_wins
FROM league_rankings
ORDER BY season, rank_by_goals, rank_by_wins
