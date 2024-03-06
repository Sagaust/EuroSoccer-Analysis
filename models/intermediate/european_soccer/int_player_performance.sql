-- Adjusted int_player_performance model to include season analysis with the correct date column name.
WITH seasonal_attributes AS (
    SELECT
        pa.player_api_id,
        EXTRACT(YEAR FROM pa.attribute_date) AS season_year, -- Adjusted to the correct column name for date
        AVG(pa.overall_rating) AS avg_overall_rating,
        AVG(pa.potential) AS avg_potential,
        AVG(pa.dribbling) AS avg_dribbling,
        AVG(pa.short_passing) AS avg_passing,
        AVG(pa.ball_control) AS avg_ball_control,
        AVG(pa.long_passing) AS avg_long_passing,
        COUNT(*) AS total_entries
    FROM {{ ref('stg_player_attributes') }} pa
    GROUP BY pa.player_api_id, season_year
),
player_info AS (
    SELECT
        p.player_api_id,
        p.player_name,
        p.birthday,
        p.height,
        p.weight,
        DATE_DIFF(CURRENT_DATE(), CAST(p.birthday AS DATE), YEAR) AS age
    FROM {{ ref('stg_players') }} p
)
SELECT
    sa.player_api_id,
    pi.player_name,
    pi.birthday,
    pi.height,
    pi.weight,
    pi.age,
    sa.season_year,
    sa.avg_overall_rating,
    sa.avg_potential,
    sa.avg_dribbling,
    sa.avg_passing,
    sa.avg_ball_control,
    sa.avg_long_passing,
    sa.total_entries
FROM seasonal_attributes sa
JOIN player_info pi ON sa.player_api_id = pi.player_api_id
ORDER BY sa.player_api_id, sa.season_year
