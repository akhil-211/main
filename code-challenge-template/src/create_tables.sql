--create yeild table to store yeild data
create Table yeild_data_table(
	yeild_year INT PRIMARY KEY,
	yeild_value INT
)

--create wx table to store weather data
create Table wx_data_table(
	station VARCHAR ( 20 ) NOT NULL,
	record_date Date NOT NULL,
	max_temp float NOT NULL,
	min_temp float NOT NULL,
	precipitate float NOT NULL,
	PRIMARY KEY (station, record_date)
	)



--create table to store avg max temperature, avg min temperature, total precipitation in cm
create Table aggregated_wx_data(
	station VARCHAR ( 20 ) NOT NULL,
	record_year varchar(4) NOT NULL,
	max_temperature float NULL,
	min_temperature float NULL,
	total_precipitation_in_cm float Null,
	PRIMARY KEY (station, record_year)
	)

