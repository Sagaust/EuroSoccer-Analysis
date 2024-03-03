WITH player_attributes AS (
    SELECT
        pa.player_api_id,
        AVG(pa.overall_rating) AS avg_overall_rating,
        AVG(pa.potential) AS avg_potential,
        -- Note: Adjustments might be needed for aggregating string fields.
        MAX(pa.preferred_foot) AS most_common_preferred_foot, -- Example adjustment
        MAX(pa.attacking_work_rate) AS most_common_attacking_work_rate, -- Example adjustment
        MAX(pa.defensive_work_rate) AS most_common_defensive_work_rate, -- Example adjustment
        COUNT(*) AS num_appearances
    FROM `austwagonproject.European_Soccer.stg_player_attributes` pa
    GROUP BY pa.player_api_id
),
player_info AS (
    SELECT
        p.player_api_id,
        p.player_name,
        p.birthday,
        p.height,
        p.weight
    FROM `austwagonproject.European_Soccer.stg_players` p
)

SELECT
    pi.player_api_id,
    pi.player_name,
    pi.birthday,
    pi.height,
    pi.weight,
    pa.avg_overall_rating,
    pa.avg_potential,
    pa.most_common_preferred_foot,
    pa.most_common_attacking_work_rate,
    pa.most_common_defensive_work_rate, 
    pa.num_appearances
FROM player_info pi
JOIN player_attributes pa ON pi.player_api_id = pa.player_api_id