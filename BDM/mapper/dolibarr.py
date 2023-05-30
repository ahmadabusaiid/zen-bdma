from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode

from pathlib import Path
import os
import sys

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as common


spark = SparkSession.builder.master("local").appName(common.spark['appName']).getOrCreate()

hdfs_host = common.hdfs['host_path']
dolibarr_invoices=spark.read.parquet(f'hdfs://{hdfs_host}/dolibarr/invoices/2023-05-26').cache()

transcations = dolibarr_invoices.select('id','ref_customer', 'totalpaid').show()
customer_details = dolibarr_invoices.select('ref_customer','firstname','lastname').distinct().show()
sales = dolibarr_invoices.select('date_creation','id',explode(dolibarr_invoices.lines).alias('product'))\
.select('date_creation','id','product.product_ref','product.price','product.qty').show()


dolibarr_products=spark.read.parquet(f'hdfs://{hdfs_host}/dolibarr/products/2023-05-26').cache()

product_prices = dolibarr_products.select('date_modification','id','price','cost_price').show()
products = dolibarr_products.select('id','label','description','type').distinct().show()