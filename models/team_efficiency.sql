-- models/team_efficiency.sql
with
    team_performance as (
        select
            t.team_api_id,
            t.team_long_name,
            count(m.match_api_id) as total_matches,
            sum(
                case
                    when
                        m.home_team_api_id = t.team_api_id
                        and m.home_team_goal > m.away_team_goal
                    then 1
                    when
                        m.away_team_api_id = t.team_api_id
                        and m.away_team_goal > m.home_team_goal
                    then 1
                    else 0
                end
            ) as wins,
            sum(
                case
                    when
                        m.home_team_api_id = t.team_api_id
                        and m.home_team_goal = m.away_team_goal
                    then 1
                    when
                        m.away_team_api_id = t.team_api_id
                        and m.away_team_goal = m.home_team_goal
                    then 1
                    else 0
                end
            ) as draws,
            sum(
                case
                    when
                        m.home_team_api_id = t.team_api_id
                        and m.home_team_goal < m.away_team_goal
                    then 1
                    when
                        m.away_team_api_id = t.team_api_id
                        and m.away_team_goal < m.home_team_goal
                    then 1
                    else 0
                end
            ) as losses
        from `austwagonproject.European_Soccer.Team` t
        join
            `austwagonproject.European_Soccer.Match` m
            on t.team_api_id = m.home_team_api_id
            or t.team_api_id = m.away_team_api_id
        group by t.team_api_id, t.team_long_name
    )

select
    *,
    (wins * 3 + draws) as total_points,
    round((wins * 3 + draws) / total_matches, 2) as efficiency
from team_performance
