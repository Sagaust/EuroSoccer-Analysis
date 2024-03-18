SELECT
    team_api_id AS team_id,
    CONCAT(team_long_name, ' - ', team_short_name) AS team_name
FROM {{ source('European_Soccer', 'Team') }}
