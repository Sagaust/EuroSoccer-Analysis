WITH match_outcomes AS (
    SELECT
        m.team_api_id,
        COUNT(*) AS total_matches,
        COUNT(CASE WHEN m.home_team_goal > m.away_team_goal THEN 1 END) AS home_wins,
        COUNT(CASE WHEN m.home_team_goal < m.away_team_goal THEN 1 END) AS home_losses,
        COUNT(CASE WHEN m.home_team_goal = m.away_team_goal THEN 1 END) AS draws,
        SUM(m.home_team_goal) AS goals_for,
        SUM(m.away_team_goal) AS goals_against
    FROM `austwagonproject.European_Soccer.Match` m
    GROUP BY m.team_api_id
    UNION ALL
    SELECT
        m.team_api_id,
        COUNT(*) AS total_matches,
        COUNT(CASE WHEN m.away_team_goal > m.home_team_goal THEN 1 END) AS away_wins,
        COUNT(CASE WHEN m.away_team_goal < m.home_team_goal THEN 1 END) AS away_losses,
        COUNT(CASE WHEN m.away_team_goal = m.home_team_goal THEN 1 END) AS draws,
        SUM(m.away_team_goal) AS goals_for,
        SUM(m.home_team_goal) AS goals_against
    FROM `austwagonproject.European_Soccer.Match` m
    GROUP BY m.team_api_id
),
aggregated_outcomes AS (
    SELECT
        team_api_id,
        SUM(total_matches) AS total_matches,
        SUM(home_wins) + SUM(away_wins) AS wins,
        SUM(home_losses) + SUM(away_losses) AS losses,
        SUM(draws) AS draws,
        SUM(goals_for) AS goals_for,
        SUM(goals_against) AS goals_against,
        SUM(goals_for) - SUM(goals_against) AS goal_difference
    FROM match_outcomes
    GROUP BY team_api_id
),
team_info AS (
    SELECT
        t.team_api_id,
        t.team_long_name,
        t.team_short_name
    FROM `austwagonproject.European_Soccer.Team` t
)

SELECT
    ti.team_long_name,
    ti.team_short_name,
    ao.total_matches,
    ao.wins,
    ao.losses,
    ao.draws,
    ao.goals_for,
    ao.goals_against,
    ao.goal_difference
FROM team_info ti
JOIN aggregated_outcomes ao ON ti.team_api_id = ao.team_api_id
ORDER BY ao.wins DESC, ao.goal_difference DESC
