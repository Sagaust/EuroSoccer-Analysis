{{ config(materialized='table') }} -- Or 'view' if preferable

WITH player_base AS (
    -- Let's combine basic player data 
    SELECT
        player_api_id,
        player_name,
        height,
        weight,
        age
    FROM {{ ref('stg_players') }}
),

player_attribute_stats AS (
    -- We can add more aggregate attributes if necessary
    SELECT
        player_api_id,
        AVG(overall_rating) AS avg_overall_rating,
        MAX(potential) AS max_potential,
          
    FROM {{ ref('stg_player_attributes') }}
    GROUP BY player_api_id
)

SELECT
    pb.player_api_id,
    pb.player_name,
    pas.avg_overall_rating,
    pas.max_potential
    height,
    weight,
    age
FROM player_base pb
JOIN player_attribute_stats pas ON pb.player_api_id = pas.player_api_id 
