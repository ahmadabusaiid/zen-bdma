from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import *
from pyspark.conf import SparkConf
from monetdb_loader import DBLoader
from pathlib import Path
import os
import sys
import math

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as common

df_rows = 250
today = '2023-06-05'
def map_to_db(today):

    try : 
        db_loader = DBLoader()
        driver_path = db_loader.get_driver_path()

        hdfs_host = common.hdfs['host_path']

        conf = SparkConf()
        conf.set("spark.jars", driver_path)

        spark = SparkSession.builder.config(conf=conf).master("local").appName(common.spark['appName']).getOrCreate()

        weatherapi_forecast =spark.read.parquet(f'hdfs://{hdfs_host}/weatherapi/forecast/{today}').cache()

        forecast = weatherapi_forecast.withColumn('daily_will_it_rain',when(col('forecast.forecastday.day.daily_will_it_rain')[0].contains(0),'false').otherwise('true'))\
        .withColumn('daily_will_it_snow',when(col('forecast.forecastday.day.daily_will_it_snow')[0].contains(0),'false').otherwise('true'))\
        .withColumn('daily_chance_of_snow',when(col('forecast.forecastday.day.daily_chance_of_snow')[0].contains(0),'false').otherwise('true'))\
        .select(col('location.name').alias('city_name'),col('location.country').alias('country'),col('location.region').alias('region'),col('location.lat').alias('latitude'),col('location.lon').alias('longitude'),col('location.tz_id').alias('time_zone'),col('forecast.forecastday.date')[0].alias('date'),col('forecast.forecastday.day.maxtemp_c')[0].alias('max_temp'),col('forecast.forecastday.day.mintemp_c')[0].alias('min_temp'),col('forecast.forecastday.day.avgtemp_c')[0].alias('avg_temp'),col('forecast.forecastday.day.maxwind_kph')[0].alias('max_wind'),col('forecast.forecastday.day.totalprecip_mm')[0].alias('total_precip'),col('forecast.forecastday.day.totalsnow_cm')[0].alias('total_snow'),col('forecast.forecastday.day.avgvis_km')[0].alias('avg_vis'),col('forecast.forecastday.day.avghumidity')[0].alias('avg_humidity'),col('forecast.forecastday.day.uv')[0].alias('uv'),col('forecast.forecastday.day.condition.text')[0].alias('condition'),col('daily_will_it_rain'),col('daily_will_it_snow'),col('daily_chance_of_snow'))\
        .withColumn('sid',concat_ws('','date','city_name'))

        forecast = forecast.select('sid','city_name','country','region','latitude','longitude','time_zone','date','max_temp','min_temp','avg_temp','max_wind','total_precip','total_snow','avg_vis','avg_humidity','daily_will_it_rain','daily_will_it_snow','daily_chance_of_snow','uv','condition')
        db_loader.write_to_table(forecast, 'weather.forecast', math.ceil(forecast.count()/df_rows))

    except:
        print('Mapping data from file to database failed.')