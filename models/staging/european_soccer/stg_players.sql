-- models/staging/european_soccer/stg_players.sql
SELECT
  player_api_id,
  player_name,
  CAST(birthday AS DATE) AS birthday, -- Convert birthdate string to date format
  height,
  weight,
  COALESCE(height, 0) AS height_clean, -- Example of handling missing values, setting them to a default
  COALESCE(weight, 0) AS weight_clean
FROM
  `austwagonproject`.`European_Soccer`.`Player`
WHERE
  player_name IS NOT NULL; -- Filter out records with no player name
