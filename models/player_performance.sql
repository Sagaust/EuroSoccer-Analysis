-- models/player_performance.sql

WITH player_performance AS (
  SELECT
    pa.player_api_id,
    AVG(pa.overall_rating) AS avg_overall_rating,
    AVG(pa.potential) AS avg_potential,
    AVG(pa.reactions) AS avg_reactions,
    COUNT(*) AS num_appearances
  FROM `austwagonproject.European_Soccer.Player_Attributes` pa
  GROUP BY pa.player_api_id
)

SELECT
  p.player_api_id,
  p.player_name,
  pp.avg_overall_rating,
  pp.avg_potential,
  pp.avg_reactions,
  pp.num_appearances
FROM `austwagonproject.European_Soccer.Player` p
JOIN player_performance pp
  ON p.player_api_id = pp.player_api_id
