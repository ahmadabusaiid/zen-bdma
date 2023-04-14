from invoke import WeatherAPIInvoker
import configs

wi = WeatherAPIInvoker()

# weather forcast
wi.query(path = 'forecast.json', ext_params = { "q": configs.weather_api["forecast"]["city"], "days" : configs.weather_api["forecast"]["days"] })

# historical weather
# wi.query(path = 'history.json', ext_params = { "q": configs.weather_api["forecast"]["city"], "dt": configs.weather_api["history"]["start_date"], "end_dt": configs.weather_api["history"]["end_date"]})



