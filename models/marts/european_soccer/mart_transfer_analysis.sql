-- Description: Summarizes player transfer data, highlighting significant transfers,
-- including player names, from and to teams.

{{ config(materialized='table') }}

WITH transfer_summary AS (
    SELECT
        pta.player_api_id,
        p.player_name,
        from_t.team_long_name AS from_team,
        to_t.team_long_name AS to_team,
        pta.season AS transfer_season
    FROM {{ ref('int_player_transfer_analysis') }} pta
    LEFT JOIN {{ ref('stg_players') }} p ON pta.player_api_id = p.player_api_id
    LEFT JOIN {{ ref('stg_team') }} from_t ON pta.previous_team = from_t.team_api_id
    LEFT JOIN {{ ref('stg_team') }} to_t ON pta.new_team = to_t.team_api_id
)

SELECT
    player_name,
    from_team,
    to_team,
    transfer_season
FROM transfer_summary
ORDER BY transfer_season, player_name
