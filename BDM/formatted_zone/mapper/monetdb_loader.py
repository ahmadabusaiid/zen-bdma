from pathlib import Path
import os
import sys

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as common

class DBLoader:

    def __init__(self):
        
        self._driver_path = common.monetdb['driver_path']
        self._host = common.monetdb['host_path']
        self._db = common.monetdb['database']
        self._username = common.monetdb['user']
        self._password = common.monetdb['password']
        self._driver = common.monetdb['driver']

    def get_driver_path (self):
        return self._driver_path
    
    def write_to_table(self, dataframe, table_name):

        dataframe.write \
        .format("jdbc") \
        .option("url", f"jdbc:{self._host}/{self._db}") \
        .option("dbtable", table_name) \
        .option("user", self._username) \
        .option("password", self._password) \
        .option("driver", self._driver) \
        .mode('append')\
        .save()

        print (f"Loaded {table_name}..")