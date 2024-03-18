SELECT
    player_api_id AS player_id,
    player_name,
    PARSE_DATE('%Y-%m-%d', SUBSTR(CAST(birthday AS STRING),0,10)) AS birthday,
    height,
    weight
FROM {{ source('European_Soccer', 'Player') }}