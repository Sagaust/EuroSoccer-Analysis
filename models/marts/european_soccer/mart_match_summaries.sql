-- mart_match_summaries.sql
-- Corrected for syntax and structured to align with BigQuery SQL standards

{{ config(materialized='table') }}

SELECT
    md.match_api_id,
    md.season,
    md.match_date,
    home_team.team_long_name AS home_team,
    away_team.team_long_name AS away_team,
    CONCAT(CAST(md.home_team_goal AS STRING), '-', CAST(md.away_team_goal AS STRING)) AS score,
    CASE
        WHEN md.home_team_goal > md.away_team_goal THEN home_team.team_long_name
        WHEN md.home_team_goal < md.away_team_goal THEN away_team.team_long_name
        ELSE 'Tie'
    END AS victorious_team,
    md.home_team_goal,
    md.away_team_goal,
    md.home_team_goal + md.away_team_goal AS number_goals
FROM {{ ref('int_match_details_enhanced') }} md
LEFT JOIN {{ ref('stg_team') }} home_team ON md.home_team_api_id = home_team.team_api_id
LEFT JOIN {{ ref('stg_team') }} away_team ON md.away_team_api_id = away_team.team_api_id
ORDER BY md.match_date DESC, md.match_api_id
