from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.conf import SparkConf
from pathlib import Path
import os
import sys

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as common
from monetdb_loader import DBLoader

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

        weatherapi_forecast = spark.read.parquet(f'hdfs://{hdfs_host}/weatherapi/forecast/{today}').cache()

        forecast = weatherapi_forecast.select(col('location.name').alias('city_name'),col('location.country').alias('country'),col('location.region').alias('region'),col('location.lat').alias('latitude'))

        
    except:
        print('Mapping data from file to database failed.')

map_to_db(today)