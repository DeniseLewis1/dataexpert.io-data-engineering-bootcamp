WITH games_data AS (
	SELECT 
		DISTINCT gd.game_id,
		gd.team_id,
		gd.team_abbreviation,
		g.game_date_est,
		CASE 
			WHEN home_team_wins = 1 THEN home_team_id
			ELSE visitor_team_id
		END AS winning_team
	FROM game_details gd
	LEFT JOIN games g
		ON gd.game_id = g.game_id
),
games_with_wins AS (
	SELECT 
		game_id,
		team_id,
		team_abbreviation,
		game_date_est,
		CASE
			WHEN team_id = winning_team THEN 1
			ELSE 0
		END AS win,
		ROW_NUMBER() OVER (PARTITION BY team_abbreviation ORDER BY game_date_est) AS game_num
	FROM games_data
),
games_window AS (
	SELECT
		team_abbreviation,
		game_num,
		SUM(win) OVER (
			PARTITION BY team_abbreviation
			ORDER BY game_num
			ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
		) AS total_wins
	FROM games_with_wins
)

SELECT
    team_abbreviation,
    MAX(total_wins) AS total_wins
FROM games_window
GROUP BY team_abbreviation
ORDER BY total_wins DESC;