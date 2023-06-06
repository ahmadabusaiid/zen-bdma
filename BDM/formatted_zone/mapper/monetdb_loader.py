from pathlib import Path
import os
import sys
from pymonetdb import mapi

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as common

class DBLoader:

    def __init__(self):
        
        self._driver_path = common.monetdb['driver_path']
        self._host = common.monetdb['host_path']
        self._port = common.monetdb['port']
        self._db = common.monetdb['database']
        self._username = common.monetdb['user']
        self._password = common.monetdb['password']
        self._driver = common.monetdb['driver']

    def get_driver_path (self):
        return self._driver_path
    
    def write_to_table(self, dataframe, table_name, repartitions = 8, mode = 'append', truncate = False):

        if repartitions <8:
             repartitions = 8
             
        dataframe.repartition(repartitions) \
        .write \
        .format("jdbc") \
        .option("url", f"jdbc:monetdb://{self._host}:{self._port}/{self._db}") \
        .option("dbtable", table_name) \
        .option("user", self._username) \
        .option("password", self._password) \
        .option("driver", self._driver) \
        .option("truncate", truncate) \
        .mode(mode)\
        .save()

        print (f"Loaded {table_name}..")
    
    def run_query(self, query):

        mapi_connection = mapi.Connection()
        mapi_connection.connect(username=self._username, password=self._password, hostname=self._host,port=self._port, database=self._db, language='sql', unix_socket=None, connect_timeout=-1)
        mapi_connection.cmd(f's{query}')

    def read_table(self, spark , query):
            df = spark.read \
                    .format("jdbc") \
                    .option("url", f"jdbc:monetdb://{self._host}:{self._port}/{self._db}") \
                    .option("driver", self._driver) \
                    .option("query", query) \
                    .option("user", self._username) \
                    .option("password", self._password) \
                    .load()
            
            return df 