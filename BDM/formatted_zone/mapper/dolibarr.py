from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode
from pyspark.conf import SparkConf
from monetdb_loader import DBLoader
from pathlib import Path
import os
import sys

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as common

db_loader = DBLoader()
driver_path = db_loader.get_driver_path()

hdfs_host = common.hdfs['host_path']
branch_id = common.global_params['branch_id']

conf = SparkConf()
conf.set("spark.jars", driver_path)

spark = SparkSession.builder.config(conf=conf).master("local").appName(common.spark['appName']).getOrCreate()

dolibarr_invoices=spark.read.parquet(f'hdfs://{hdfs_host}/{branch_id}/dolibarr/invoices/2023-05-26').cache()

transcations = dolibarr_invoices.select('date_creation', 'id','ref_customer', 'totalpaid')
db_loader.write_to_table(transcations, 'transaction')

customer_details = dolibarr_invoices.select('ref_customer','firstname','lastname').distinct()
db_loader.write_to_table(customer_details, 'customer')

sales = dolibarr_invoices.select('date_creation','id',explode(dolibarr_invoices.lines).alias('product'))\
.select('date_creation','id','product.product_ref','product.price','product.qty')
db_loader.write_to_table(sales, 'sales')

dolibarr_products=spark.read.parquet(f'hdfs://{hdfs_host}/{branch_id}/dolibarr/products/2023-05-26').cache()

product_prices = dolibarr_products.select('date_modification','id','price','cost_price')
db_loader.write_to_table(product_prices, 'product_prices')

products = dolibarr_products.select('id','label','description','type').distinct()
db_loader.write_to_table(products, 'product')