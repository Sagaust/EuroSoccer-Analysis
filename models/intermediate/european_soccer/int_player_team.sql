WITH home_player_team AS(
SELECT
league_id, season, DATE_TRUNC(game_date, MONTH) as month_year_game, home_team_id AS team_id, match_id, player_id
FROM {{ ref('stg_match') }}
UNPIVOT(player_id FOR metric IN (home_player_1, home_player_2, home_player_3, home_player_4, home_player_5, home_player_6, home_player_7, home_player_8, home_player_9, home_player_10, home_player_11))
),

away_player_team AS(
SELECT
league_id, season, DATE_TRUNC(game_date, MONTH) as month_year_game, away_team_id AS team_id, match_id, player_id
FROM {{ ref('stg_match') }}
UNPIVOT(player_id FOR metric IN (away_player_1, away_player_2, away_player_3, away_player_4, away_player_5, away_player_6, away_player_7, away_player_8, away_player_9, away_player_10, away_player_11))
),

union_player AS (
SELECT
*
FROM home_player_team
UNION ALL
SELECT
*
FROM away_player_team
),

calculate_goal_player AS(
  SELECT
    match_id,
    CAST(player_id AS INT64) AS player_id,
    COUNT(player_id) AS nb_goal
  FROM
    {{ ref('stg_match') }},
    UNNEST(REGEXP_EXTRACT_ALL(goal, r'<player1>(\d+)</player1>')) as player_id
  WHERE
    goal IS NOT NULL
  GROUP BY
    match_id, goal, player_id
)

SELECT
  player.*,
  goal.nb_goal
FROM union_player AS player
LEFT JOIN calculate_goal_player AS goal ON player.match_id = goal.match_id AND player.player_id = goal.player_id