SELECT event_hour, SUM(num_hits) AS num_events, COUNT(ip) AS num_users, SUM(num_hits) / COUNT(ip) AS avg_web_events
FROM sessionized_events
GROUP BY event_hour


WITH aggregated_data AS (
	SELECT event_hour, host, SUM(num_hits) AS num_events, COUNT(ip) AS num_users, SUM(num_hits) / COUNT(ip) AS avg_web_events
	FROM sessionized_events
	GROUP BY event_hour, host
)

SELECT host, SUM(num_events) AS num_events, SUM(num_users) AS num_users, SUM(num_events) / SUM(num_users) AS avg_web_events
FROM aggregated_data
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host