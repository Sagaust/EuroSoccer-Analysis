WITH core_table AS (
SELECT
  CONCAT(players_table.match_id, '-',team.team_name) AS pk_id,
  players_table.player_id,
  player.player_name,
  players_table.season,
  players_table.month_year_game,
  league.country_name,
  league.league_name,
  team.team_name,
  players_table.match_id,
  COALESCE(players_table.nb_goal, 0) AS nb_goal,
  avg_overall_rating,
  avg_potential,
  player_attack_score,
  player_physical_score,
  player_defense_score,
  player_goalkeeping_score,
  preferred_foot
FROM {{ ref('int_player_team') }} AS players_table
LEFT JOIN {{ ref('stg_player') }} AS player ON players_table.player_id = player.player_id
LEFT JOIN {{ ref('int_league_country') }} AS league USING (league_id)
LEFT JOIN {{ ref('stg_team') }} AS team USING (team_id)
LEFT JOIN {{ ref('int_player_attributes') }} AS perf ON players_table.player_id = perf.player_id AND players_table.season = perf.season
WHERE players_table.player_id IS NOT NULL
)

SELECT
  core_table.*,
  match.score_class,
  match.category
FROM core_table
LEFT JOIN {{ ref('core_matches_teams_attributes') }} AS match USING (pk_id)