SELECT
  CONCAT(match.match_id, '-',team.team_name) AS pk_id,
  league.country_name,
  league.league_name,
  match.season,
  match.match_id,
  match.team_id,
  team.team_name,
  match.goal,
  match.category,
  match.score,
  COALESCE(team2.team_name, 'Draw') AS victorious_team,
  CASE
    WHEN team2.team_name = team.team_name THEN 'Victory'
    WHEN team2.team_name IS NULL THEN 'Draw'
    ELSE 'Defeat'
  END AS score_class,
  team_attack_score,
  team_defense_score,
  team_ball_control_score,
  (team_attack_score + team_defense_score + team_ball_control_score) / 3 AS team_score,
  buildUpPlaySpeedClass,
  buildUpPlayDribblingClass,
  buildUpPlayPassingClass,
  buildUpPlayPositioningClass,
  chanceCreationPassingClass,
  chanceCreationCrossingClass,
  chanceCreationShootingClass,
  chanceCreationPositioningClass,
  defencePressureClass,
  defenceAggressionClass,
  defenceTeamWidthClass,
  defenceDefenderLineClass
FROM {{ ref('int_matches_teams') }} AS match
LEFT JOIN {{ ref('int_league_country') }} AS league USING (league_id)
LEFT JOIN {{ ref('stg_team') }} AS team USING (team_id)
LEFT JOIN {{ ref('stg_team') }} AS team2 ON match.victorious_team_id = CAST(team2.team_id AS STRING)
LEFT JOIN {{ ref('int_team_attributes') }} AS attributes ON match.team_id = attributes.team_id AND match.season = attributes.season
ORDER BY match.season, match.match_id, match.category DESC
