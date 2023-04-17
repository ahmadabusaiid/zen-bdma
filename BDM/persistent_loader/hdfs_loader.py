from abc import abstractmethod
import sys
import os 
from pathlib import Path
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs as configs

class Loader:

    def __init__(self, uri):
        self._uri = uri

    @abstractmethod
    def write(self):
        pass

class HDFSLoader(Loader):

    def __init__(self):
        
        super().__init__(configs.hdfs['host_path'])
        self._spark = SparkSession.builder.appName("HDFSLoader").getOrCreate()
    
    def write(self, directory):

        files = os.listdir(directory)

        i = 0
        for file in files:
            jsons_df = self._spark.read.json( f'{directory}/{file}')

            if i == 0:
                jsons_df.write.parquet(f'{directory}.parquet')
            else :
                jsons_df.write.mode('append').parquet(f'hdfs://{self._uri}/{directory}.parquet')
            
            i += 1

hdfs = HDFSLoader()
input_dir = configs.global_params['temp_dir']
path = 'weatherapi/forecast/2023-04-17'
hdfs.write(f'{Path.home()}/{input_dir}/{path}')