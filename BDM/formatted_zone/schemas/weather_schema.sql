
CREATE SCHEMA weather; 

CREATE TABLE weather.forecast (
    sid string PRIMARY KEY, --name+forecast.forecastday.date
    city_name string NOT NULL,
    country string,
    region string, 
    latitude float,
    longitude float, 
    time_zone string, 
    date date NOT NULL, --forecast.forecastday.date
    max_temp float, --forecast.forecastday.day.maxtemp_c
    min_temp float, --forecast.forecastday.day.mintemp_c 
    avg_temp float, 
    max_wind float, 
    total_precip float, 
    total_snow float,
    avg_vis float, 
    avg_humidity float, 
    daily_will_it_rain boolean, 
    daily_will_it_snow boolean, 
    daily_chance_of_snow boolean, 
    uv float, 
    condition string
); 
