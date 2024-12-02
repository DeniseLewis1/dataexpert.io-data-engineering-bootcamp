INSERT INTO user_devices_cumulated
WITH yesterday AS (
	SELECT *
	FROM user_devices_cumulated
	WHERE date = DATE('2023-01-30')
),
	today AS (
		SELECT 
			user_id,
			e.device_id,
			browser_type,
			DATE(CAST(event_time AS TIMESTAMP)) AS date_active
		FROM events e
		LEFT JOIN devices d
			ON e.device_id = d.device_id
		WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31') AND user_id IS NOT NULL and e.device_id IS NOT NULL
		GROUP BY user_id, e.device_id, browser_type, DATE(CAST(event_time AS TIMESTAMP))
	)

SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(t.device_id, y.device_id) AS device_id,
	COALESCE(t.browser_type, y.browser_type) AS browser_type,
	CASE
		WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.device_activity_datelist
		ELSE ARRAY[t.date_active] || y.device_activity_datelist
	END AS device_activity_datelist,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
	ON t.user_id = y.user_id AND t.device_id = y.device_id