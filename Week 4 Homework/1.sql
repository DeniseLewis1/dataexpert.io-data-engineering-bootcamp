CREATE TABLE players_state_tracking (
	player_name TEXT,
	first_active_season INTEGER,
	last_active_season INTEGER,
	season_state TEXT,
	season INTEGER,
	PRIMARY KEY (player_name, season)
);


INSERT INTO players_state_tracking
WITH previous_year AS (
	SELECT *
	FROM players_state_tracking
	WHERE season = 2009
),
current_year AS (
	SELECT
		player_name,
		season AS current_season,
		COUNT(1)
	FROM player_seasons
	WHERE season = 2010
	GROUP BY player_name, season
)

SELECT
	COALESCE(c.player_name, p.player_name) AS player_name,
	COALESCE(p.first_active_season, c.current_season) AS first_active_season,
	COALESCE(c.current_season, p.last_active_season) AS last_active_season,
	CASE 
		WHEN p.player_name IS NULL THEN 'New'
		WHEN p.last_active_season = c.current_season - 1 THEN 'Continued Playing'
		WHEN p.last_active_season < c.current_season - 1 THEN 'Returned from Retirement'
		WHEN c.current_season IS NULL AND p.last_active_season = p.season THEN 'Retired'
		ELSE 'Stayed Retired'
	END AS season_state,
	COALESCE(c.current_season, p.season + 1) AS season
FROM current_year c
FULL OUTER JOIN previous_year p
	ON c.player_name = p.player_name