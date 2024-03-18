SELECT
    player_id,
    CASE
      WHEN EXTRACT(MONTH FROM date_attributes)>8 THEN CONCAT(EXTRACT(YEAR FROM date_attributes),'/',EXTRACT(YEAR FROM date_attributes)+1)
    ELSE CONCAT(EXTRACT(YEAR FROM date_attributes)-1,'/',EXTRACT(YEAR FROM date_attributes))
    END AS season,
    ROUND((AVG(crossing) + AVG(finishing) + AVG(heading_accuracy) + AVG(short_passing) + AVG(volleys) + AVG(dribbling) + AVG(curve) + AVG(free_kick_accuracy) + AVG(long_passing) + AVG(ball_control) + AVG(positioning) + AVG(vision) + AVG(penalties) + AVG(shot_power) + AVG(long_shots)) / 15, 0) AS player_attack_score,
    ROUND((AVG(aggression) + AVG(interceptions) + AVG(marking) + AVG(standing_tackle)) / 4, 0) AS player_defense_score,
    ROUND((AVG(acceleration) + AVG(sprint_speed) + AVG(agility) + AVG(reactions) + AVG(balance) + AVG(jumping) + AVG(stamina) + AVG(strength)) / 8, 0) AS player_physical_score,
    ROUND((AVG(gk_diving) + AVG(gk_handling) + AVG(gk_kicking) + AVG(gk_positioning) + AVG(gk_reflexes)) / 5, 0) AS player_goalkeeping_score,
    MAX(preferred_foot) AS preferred_foot,
    ROUND(AVG(overall_rating), 0) AS avg_overall_rating,
    ROUND(AVG(potential), 0) AS avg_potential
  FROM {{ ref('stg_player_attributes') }}
  GROUP BY player_id, season--, player_attack_score, player_defense_score, player_physical_score, player_goalkeeping_score
 
