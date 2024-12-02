CREATE TABLE user_devices_cumulated (
	user_id NUMERIC,
	device_id NUMERIC,
	browser_type TEXT,
	dates_active DATE[],
	date DATE,
	PRIMARY KEY (user_id, browser_type, date)
)