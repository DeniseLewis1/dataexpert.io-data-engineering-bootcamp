INSERT INTO hosts_cumulated
WITH yesterday AS (
	SELECT *
	FROM hosts_cumulated
	WHERE date = DATE('2023-01-30')
),
	today AS (
		SELECT 
			DISTINCT host,
			DATE(CAST(event_time AS TIMESTAMP)) AS date_active
		FROM events
		WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
	)

SELECT 
	COALESCE(t.host, y.host) AS host,
	CASE
		WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.host_activity_datelist
		ELSE ARRAY[t.date_active] || y.host_activity_datelist
	END AS dates_active,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
	ON t.host = y.host

