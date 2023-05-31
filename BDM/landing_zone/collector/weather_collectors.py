from collector.invoke import WeatherAPIInvoker
import os
from pathlib import Path
import sys
 
sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as configs

def collect():
    wi = WeatherAPIInvoker()
    wi.query(path = 'forecast.json', ext_params = { "q": configs.weather_api["forecast"]["city"], "days" : configs.weather_api["forecast"]["days"] })

# historical weather
# wi.query(path = 'history.json', ext_params = { "q": configs.weather_api["forecast"]["city"], "dt": configs.weather_api["history"]["start_date"], "end_dt": configs.weather_api["history"]["end_date"]})



