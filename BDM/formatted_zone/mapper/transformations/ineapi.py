from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import split
from pyspark.conf import SparkConf
from pathlib import Path
import os
import sys
import math

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as common
from monetdb_loader import DBLoader

df_rows = 250
def map_to_db(today):

    try : 
        db_loader = DBLoader()
        driver_path = db_loader.get_driver_path()

        hdfs_host = common.hdfs['host_path']

        conf = SparkConf()
        conf.set("spark.jars", driver_path)

        spark = SparkSession.builder.config(conf=conf).master("local").appName(common.spark['appName']).getOrCreate()

        ine =spark.read.parquet(f'hdfs://{hdfs_host}/ineapi/population/{today}').cache()

        population = ine.select(col('COD').alias('cod'),col('Nombre').alias('title'),col('Data.Fecha')[0].alias("epoch"),col('Data.Anyo')[0].alias('for_year'),col('Data.Valor')[0].alias('population_count'))\
                    .withColumn('region', when(col('title').contains('Nacional'),'National').otherwise(split('title','\\.')[1]))\
                    .withColumn('type', when(col('title').contains('Total.'),'Total').otherwise(split('title','\\.')[2]))\
                    .withColumn('segment', when(col('type').contains('Hombres'),'Male').when(col('type').contains('Mujeres'),'Female').otherwise('Total'))\
                    .withColumn('date',from_unixtime(col('epoch')/1000,"yyyy-MM-dd").alias("date"))\
                    .withColumn('sid',concat_ws('','cod','date'))\
                    .select('sid','for_year','date','cod','region','segment','population_count')

        db_loader.write_to_table(population, 'demographics.population_details', math.ceil(population.count()/df_rows))
 
    except:
        print('Mapping data from file to database failed.')