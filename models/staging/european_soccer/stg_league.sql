SELECT
    id AS league_id,
    country_id,
    name AS league_name
FROM {{ source('European_Soccer', 'League') }}
