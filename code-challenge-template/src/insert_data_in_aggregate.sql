--Get Average maximum temperature (in degrees Celsius), Average minimum temperature (in degrees Celsius) and Total accumulated precipitation in Cm
select station, extract(year from record_date) as record_year, round(avg(NULLIF(max_temp, '-9999'))::numeric, 2) as max_temp_per_station, 
round(avg(NULLIF(min_temp, '-9999'))::numeric, 2) as min_temp_per_station, sum(NULLIF(precipitate, '-9999')) / 10 as total_precipitation_in_cm
from wx_data_table
group by station, extract(year from record_date)

--Insert into table Average minimum temperature 
insert into aggregated_wx_data (station, record_year, max_temperature, min_temperature, total_precipitation_in_cm)
select station, extract(year from record_date) as record_year, round(avg(NULLIF(max_temp, '-9999'))::numeric, 2) as max_temperature, 
round(avg(NULLIF(min_temp, '-9999'))::numeric, 2) as min_temperature, sum(NULLIF(precipitate, '-9999')) / 10 as total_precipitation_in_cm
from wx_data_table
group by station, extract(year from record_date)