SELECT
  league_id,
  league_name,
  country_name
FROM {{ ref('stg_league') }}
LEFT JOIN {{ ref('stg_country') }} USING (country_id)