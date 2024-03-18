SELECT
  team_id,
  team_name,
  CASE
    WHEN EXTRACT(MONTH FROM team_attribute_date)>8 THEN CONCAT(EXTRACT(YEAR FROM team_attribute_date),'/',EXTRACT(YEAR FROM team_attribute_date)+1)
    ELSE CONCAT(EXTRACT(YEAR FROM team_attribute_date)-1,'/',EXTRACT(YEAR FROM team_attribute_date))
  END AS season,
  ROUND((AVG(buildUpPlaySpeed) + AVG(buildUpPlayPassing) + AVG(chanceCreationShooting)) / 3, 0) AS team_attack_score,
  ROUND((AVG(defencePressure) + AVG(defenceAggression) + AVG(defenceTeamWidth)) / 3, 0) AS team_defense_score,
  ROUND((AVG(buildUpPlayPassing) + AVG(chanceCreationCrossing)) / 2, 0) AS team_ball_control_score,
  MAX(buildUpPlaySpeedClass) AS buildUpPlaySpeedClass,
  MAX(buildUpPlayDribblingClass) AS buildUpPlayDribblingClass,
  MAX(buildUpPlayPassingClass) AS buildUpPlayPassingClass,
  MAX(buildUpPlayPositioningClass) AS buildUpPlayPositioningClass,
  MAX(chanceCreationPassingClass) AS chanceCreationPassingClass,
  MAX(chanceCreationCrossingClass) AS chanceCreationCrossingClass,
  MAX(chanceCreationShootingClass) AS chanceCreationShootingClass,
  MAX(chanceCreationPositioningClass) AS chanceCreationPositioningClass,
  MAX(defencePressureClass) AS defencePressureClass,
  MAX(defenceAggressionClass) AS defenceAggressionClass,
  MAX(defenceTeamWidthClass) AS defenceTeamWidthClass,
  MAX(defenceDefenderLineClass) AS defenceDefenderLineClass
FROM {{ ref('stg_team_attributes') }}
LEFT JOIN {{ ref('stg_team') }} USING (team_id)
GROUP BY team_id, team_name, season