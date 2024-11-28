INSERT INTO actors
WITH yesterday AS (
	SELECT * FROM actors
	WHERE current_year = 2020
),
	today AS (
		SELECT
			actor,
			actorid,
			ARRAY_AGG(ROW(film, year, votes, rating, filmid)::films) as films,
			MAX(year) as year,
			AVG(rating) AS rating
		FROM actor_films
		WHERE year = 2021
		GROUP BY actorid, actor
	)

SELECT
	COALESCE(t.actor, y.actor) AS actor,
	COALESCE(t.actorid, y.actorid) AS actorid,
	CASE
		WHEN y.films IS NULL THEN t.films
		WHEN t.year IS NOT NULL THEN y.films || t.films
		ELSE y.films
	END AS films,
	CASE
		WHEN t.year IS NOT NULL THEN
			CASE
				WHEN t.rating > 8 THEN 'star'
				WHEN t.rating > 7 THEN 'good'
				WHEN t.rating > 6 THEN 'average'
				ELSE 'bad'
			END::quality_class
		ELSE y.quality_class
	END AS quality_class,
	COALESCE(t.year, y.current_year + 1) AS current_year,
	CASE
		WHEN t.year IS NOT NULL THEN TRUE
		ELSE FALSE
	END AS is_active
	
FROM today t FULL OUTER JOIN yesterday y
	ON t.actorid = y.actorid