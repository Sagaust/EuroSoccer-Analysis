-- Description: Aggregates league-wide statistics from int_league_analytics,
-- offering insights into the most competitive leagues, trends in goals scored over seasons,
-- and comparisons of league performance metrics.

{{ config(materialized='table') }}

WITH league_performance AS (
    SELECT
        league_name,
        country_name,
        season,
        SUM(total_goals) AS total_goals,
        AVG(home_wins) AS avg_home_wins,
        AVG(away_wins) AS avg_away_wins,
        AVG(draws) AS avg_draws,
        SUM(total_matches) AS total_matches,
        AVG(goals_conceded_at_home) AS avg_goals_conceded_at_home,
        AVG(goals_conceded_away) AS avg_goals_conceded_away
    FROM {{ ref('int_league_analytics') }}
    GROUP BY league_name, country_name, season
),

league_ranking AS (
    SELECT
        league_name,
        RANK() OVER (PARTITION BY season ORDER BY total_goals DESC) AS rank_by_goals,
        RANK() OVER (PARTITION BY season ORDER BY total_matches DESC) AS rank_by_matches,
        season
    FROM league_performance
)

SELECT
    lp.league_name,
    lp.country_name,
    lp.season,
    lp.total_goals,
    lp.avg_home_wins,
    lp.avg_away_wins,
    lp.avg_draws,
    lp.total_matches,
    lp.avg_goals_conceded_at_home,
    lp.avg_goals_conceded_away,
    lr.rank_by_goals,
    lr.rank_by_matches
FROM league_performance lp
JOIN league_ranking lr ON lp.league_name = lr.league_name AND lp.season = lr.season
ORDER BY lp.season, lr.rank_by_goals, lr.rank_by_matches
