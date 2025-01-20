CREATE TABLE games_dashboard AS

WITH game_data AS (
	SELECT gd.game_id, gd.team_id, gd.team_abbreviation, gd.player_id, gd.player_name, gd.pts, g.season, g.home_team_wins,
		CASE
			WHEN home_team_wins = 1 THEN home_team_id
			ELSE visitor_team_id
		END AS winning_team,
		ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.player_id) AS row_num
	FROM game_details gd
	LEFT JOIN games g
		ON gd.game_id = g.game_id
),
deduped AS (
	SELECT *
	FROM game_data gd
	WHERE gd.row_num = 1
),
games_augemented AS (
	SELECT
		COALESCE(player_name, 'unknown') AS player_name,
		COALESCE(team_abbreviation, 'unknown') AS team_abbreviation,
		COALESCE(season, 0) AS season,
		game_id,
		pts,
		CASE
			WHEN home_team_wins = 1 THEN game_id
		END AS game_id_won
	FROM deduped d
	WHERE pts IS NOT NULL
)
SELECT 
	CASE
		WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 0 THEN 'player_name__team_abbreviation'
		WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN 'player_name__season'
		WHEN GROUPING(team_abbreviation) = 0 THEN 'team_abbreviation'
	END AS aggregation_level,
	COALESCE(player_name, '(overall)') AS player_name,
	COALESCE(team_abbreviation, '(overall)') AS team_abbreviation,
	COALESCE(season, 0) AS season,
	SUM(pts) AS total_pts,
	COUNT(DISTINCT game_id_won) AS total_wins
FROM games_augemented
GROUP BY GROUPING SETS (
	(player_name, team_abbreviation),
	(player_name, season),
	(team_abbreviation)
);



-- Q1: Who scored the most points playing for one team?
SELECT player_name, team_abbreviation, total_pts
FROM games_dashboard
WHERE aggregation_level = 'player_name__team_abbreviation'
ORDER BY total_pts DESC
LIMIT 1;


-- Q2: Who scored the most points in one season?
SELECT player_name, season, total_pts
FROM games_dashboard
WHERE aggregation_level = 'player_name__season'
ORDER BY total_pts DESC
LIMIT 1;


-- Q3: Which team has won the most games?
SELECT team_abbreviation, total_wins
FROM games_dashboard
WHERE aggregation_level = 'team_abbreviation'
ORDER BY total_wins DESC
LIMIT 1;