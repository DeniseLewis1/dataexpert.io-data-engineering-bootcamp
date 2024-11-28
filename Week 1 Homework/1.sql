CREATE TYPE films AS (
	film TEXT,
	year INTEGER,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	films films[],
	quality_class quality_class,
	current_year INTEGER,
	is_active BOOLEAN,
	PRIMARY KEY(actorid, current_year)
);