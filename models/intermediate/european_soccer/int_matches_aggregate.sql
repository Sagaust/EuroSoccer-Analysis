with
    home_player_team as (
        select league_id, season, home_team_api_id as team_id, match_api_id, player_id
        from
            {{ ref("stg_matches") }},
            unnest(
                [
                    home_player_1,
                    home_player_2,
                    home_player_3,
                    home_player_4,
                    home_player_5,
                    home_player_6,
                    home_player_7,
                    home_player_8,
                    home_player_9,
                    home_player_10,
                    home_player_11
                ]
            ) as player_id
        group by league_id, season, home_team_api_id, match_api_id, player_id
    ),

    away_player_team as (
        select league_id, season, away_team_api_id as team_id, match_api_id, player_id
        from
            {{ ref("stg_matches") }},
            unnest(
                [
                    away_player_1,
                    away_player_2,
                    away_player_3,
                    away_player_4,
                    away_player_5,
                    away_player_6,
                    away_player_7,
                    away_player_8,
                    away_player_9,
                    away_player_10,
                    away_player_11
                ]
            ) as player_id
        group by league_id, season, away_team_api_id, match_api_id, player_id
    ),

    union_home_away_player as (
        select *
        from home_player_team
        union all
        select *
        from away_player_team
    ),

    player_table as (
        select
            pt.player_id,
            sps.player_name,
            pt.season,
            c.country_name,
            l.league_name,
            t.team_long_name as team_name,
            pt.match_api_id
        from union_home_away_player pt
        left join {{ ref("stg_player_summary") }} sps on pt.player_id = sps.player_api_id
        left join {{ ref("stg_league") }} l on pt.league_id = l.league_id
        left join {{ ref("stg_country") }} c on l.country_id = c.country_id
        left join {{ ref("stg_team") }} t on pt.team_id = t.team_api_id
        where pt.player_id is not null
    )

select *
from player_table
