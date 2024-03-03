-- models/my_new_project/staging/european_soccer/stg_team_attributes.sql

WITH source AS (
    SELECT
        team_api_id AS team_id,
        team_fifa_api_id,
        buildUpPlaySpeed,
        buildUpPlaySpeedClass,
        buildUpPlayDribbling,
        buildUpPlayDribblingClass,
        buildUpPlayPassing,
        buildUpPlayPassingClass,
        buildUpPlayPositioningClass,
        chanceCreationPassing,
        chanceCreationPassingClass,
        chanceCreationCrossing,
        chanceCreationCrossingClass,
        chanceCreationShooting,
        chanceCreationShootingClass,
        chanceCreationPositioningClass,
        defencePressure,
        defencePressureClass,
        defenceAggression,
        defenceAggressionClass,
        defenceTeamWidth,
        defenceTeamWidthClass,
        defenceDefenderLineClass,
        date
    FROM {{ source('European_Soccer', 'Team_Attributes') }}
)

SELECT
    team_id,
    team_fifa_api_id,
    buildUpPlaySpeed,
    buildUpPlaySpeedClass,
    buildUpPlayDribbling,
    buildUpPlayDribblingClass,
    buildUpPlayPassing,
    buildUpPlayPassingClass,
    buildUpPlayPositioningClass,
    chanceCreationPassing,
    chanceCreationPassingClass,
    chanceCreationCrossing,
    chanceCreationCrossingClass,
    chanceCreationShooting,
    chanceCreationShootingClass,
    chanceCreationPositioningClass,
    defencePressure,
    defencePressureClass,
    defenceAggression,
    defenceAggressionClass,
    defenceTeamWidth,
    defenceTeamWidthClass,
    defenceDefenderLineClass,
    CAST(date AS DATE) AS attribute_date -- Ensuring date is in DATE format for consistency
FROM source
