WITH lebron_games AS (
	SELECT
		DISTINCT gd.game_id,
		g.game_date_est,
		gd.pts,
		CASE
			WHEN gd.pts > 10 THEN 1
			ELSE 0
		END AS points_over_ten
	FROM game_details gd
	LEFT JOIN games g
		ON gd.game_id = g.game_id
	WHERE gd.player_name = 'LeBron James'
),
streak_groups AS (
	SELECT 
		game_id,
	    game_date_est,
	    pts,
	    points_over_ten,
	    ROW_NUMBER() OVER (ORDER BY game_date_est) - SUM(points_over_ten) OVER (ORDER BY game_date_est) AS streak_group
	FROM lebron_games
),
streak_lengths AS (
    SELECT 
        streak_group,
        COUNT(*) AS streak_length
    FROM streak_groups
    WHERE points_over_ten = 1
    GROUP BY streak_group
)
SELECT 
    MAX(streak_length) AS longest_streak
FROM streak_lengths