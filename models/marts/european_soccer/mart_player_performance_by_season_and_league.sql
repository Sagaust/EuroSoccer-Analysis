-- models/marts/european_soccer/mart_player_performance_season_league.sql
{{ config(materialized='table') }}

with
    match_performance as (
        select
            ima.player_name,
            ima.season,
            ima.league_name,
            ima.team_name,
            ima.match_api_id,
            ipp.avg_overall_rating,
            ipp.avg_potential,
            ipp.avg_dribbling,
            ipp.avg_passing,
            -- Include other metrics from int_player_performance as necessary
            ipp.avg_ball_control,
            ipp.avg_long_passing
        from {{ ref("int_matches_aggregate") }} as ima
        join
            {{ ref("int_player_performance") }} as ipp
            on ima.player_name = ipp.player_name
    ),
    aggregated_performance as (
        select
            player_name,
            season,
            league_name,
            avg(avg_overall_rating) as average_overall_rating,
            avg(avg_potential) as average_potential,
            avg(avg_dribbling) as average_dribbling,
            avg(avg_passing) as average_passing,
            -- Include averages for other relevant metrics
            avg(avg_ball_control) as average_ball_control,
            avg(avg_long_passing) as average_long_passing,
            count(distinct match_api_id) as matches_played
        from match_performance
        group by player_name, season, league_name
    )
select *
from aggregated_performance
order by player_name, season, league_name
