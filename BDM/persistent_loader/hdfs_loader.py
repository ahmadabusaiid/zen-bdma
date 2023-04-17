from abc import abstractmethod
import sys
import os 
from pathlib import Path
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs as configs

class Loader:

    def __init__(self, uri):
        self._uri = f'{uri}/user/bdm'

    @abstractmethod
    def write(self):
        pass

class HDFSLoader(Loader):

    def __init__(self):
        
        super().__init__(configs.hdfs['host_path'])
        self._spark = SparkSession.builder.appName("HDFSLoader").getOrCreate()
    
    def write(self, directory):

        input_dir = configs.global_params['temp_dir']
        files = os.listdir(f'{Path.home()}/{input_dir}/{directory}')

        i = 0
        for file in files:
            jsons_df = self._spark.read.option("multiline","true").json( f'{directory}/{file}')

            if i == 0:
                jsons_df.write.parquet(f'hdfs://{self._uri}/{directory}')
            else :
                jsons_df.write.mode('append').parquet(f'hdfs://{self._uri}/{directory}')
            
            i += 1

hdfs = HDFSLoader()

path = 'weatherapi/forecast/2023-04-17'
hdfs.write(path)