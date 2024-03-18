SELECT
    id AS country_id,
    name AS country_name
FROM {{ source('European_Soccer', 'Country') }}
