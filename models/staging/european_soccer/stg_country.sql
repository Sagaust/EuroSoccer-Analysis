-- models/my_new_project/staging/european_soccer/stg_country.sql

WITH source AS (
    SELECT
        id,
        name AS country_name
    FROM {{ source('European_Soccer', 'Country') }}
)

SELECT
    id AS country_id,
    country_name
FROM source
