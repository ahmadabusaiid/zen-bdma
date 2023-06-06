from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import *
from pyspark.conf import SparkConf
from pathlib import Path
import os
import sys
import math

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as common
from monetdb_loader import DBLoader

df_rows = 250
def map_to_db(today, branch_id):

    try : 
        db_loader = DBLoader()
        driver_path = db_loader.get_driver_path()

        hdfs_host = common.hdfs['host_path']

        conf = SparkConf()
        conf.set("spark.jars", driver_path)

        spark = SparkSession.builder.config(conf=conf).master("local").appName(common.spark['appName']).getOrCreate()

        dolibarr_invoices=spark.read.parquet(f'hdfs://{hdfs_host}/{branch_id}/dolibarr/invoices/{today}').cache()
        dolibarr_products=spark.read.parquet(f'hdfs://{hdfs_host}/{branch_id}/dolibarr/products/{today}').cache()
        dolibarr_stock_in=spark.read.parquet(f'hdfs://{hdfs_host}/{branch_id}/dolibarr/stockmovements/{today}').cache()
        dolibarr_inventory=spark.read.parquet(f'hdfs://{hdfs_host}/{branch_id}/dolibarr/inventory/{today}').cache()

        tables_to_load = common.global_params['load_tables']

        product_prices = None

        for i in tables_to_load:

            if i == 'products':

                ## Products table -> update
                products = dolibarr_products.select(col('id').alias('product_id'),'label','description','type').distinct().withColumn('branch_id',lit(branch_id))
                ex_products = db_loader.read_table(spark, 'SELECT * FROM client.products;')

                def update_ex_products(product):
                    
                    if ex_products.filter("product_id = $product", product=product['product_id']) :

                        ex_products.withColumn('label', when(col('product_id')== product['product_id'], product['label'])) \
                        .withColumn('description', when(col('product_id')== product['product_id'], product['description'])) \
                        .withColumn('type', when(col('product_id')== product['product_id'], product['type'])) \
                        .withColumn('branch_id', when(col('product_id')== product['product_id'], product['branch_id']))
                    else:
                        ex_products = ex_products.union(product)

                products.foreach(update_ex_products)
                
                db_loader.write_to_table(ex_products, 'client.products', math.ceil(ex_products.count()/df_rows),'overwrite', True)
            
            elif i == 'stocks':

                ## Stocks table -> append
                stocks = dolibarr_stock_in.select(col('id').alias('stock_id'),col('date_creation').alias('date'),col('product_id').alias('product_id'),col('qty').alias('quantity'),col('eatby').alias('expiry_date')).withColumn('branch_id',lit(branch_id))
                db_loader.write_to_table(stocks, 'client.stocks', math.ceil(stocks.count()/df_rows))
            
            elif i == 'inventory':


                ## Inventory table -> append
                inventory = dolibarr_inventory.select('stock_id','date',col('qty').alias('quantity')).withColumn('branch_id',lit(branch_id))
                db_loader.write_to_table(inventory, 'client.inventory', math.ceil(inventory.count()/df_rows))
            
            elif i == 'product_prices' :

                ## Product prices table -> append
                if product_prices == None:
                    product_prices = dolibarr_products.select(col('date_modification').alias('date'),col('id').alias('product_id'),col('price').alias('selling_price'),'cost_price',col('fk_unit').alias('stock_id')).withColumn('sid',concat_ws('','date','product_id')).withColumn('branch_id',lit(branch_id))
                    product_prices = product_prices.select('sid','date','product_id','selling_price','cost_price','stock_id','branch_id')
                db_loader.write_to_table(product_prices, 'client.product_prices', math.ceil(product_prices.count()/df_rows))

            elif i == 'customers':

                ## Customers table -> update
                customer_details = dolibarr_invoices.select(col('ref_customer').alias('customer_id'),col('firstname').alias('first_name'),col('lastname').alias('last_name')).distinct().withColumn('branch_id',lit(branch_id))
                ex_customer_details = db_loader.read_table(spark, 'SELECT * FROM client.customers;')

                def update_ex_customer_details(customer):
                    
                    if ex_customer_details.filter("customer_id = $customer", customer=customer['customer_id']) :

                        ex_customer_details.withColumn('first_name', when(col('customer_id')== customer['customer_id'], customer['first_name'])) \
                        .withColumn('last_name', when(col('customer_id')== customer['customer_id'], customer['last_name']))\
                        .withColumn('branch_id', when(col('customer_id')== customer['customer_id'], customer['branch_id']))
                    else:
                        ex_customer_details = ex_customer_details.union(customer)

                customer_details.foreach(update_ex_customer_details)
                db_loader.write_to_table(ex_customer_details, 'client.customers', math.ceil(ex_customer_details.count()/df_rows),'overwrite', True)

            elif i == 'transactions':
                ## Transaction table -> append
                transcations = dolibarr_invoices.select(col('id').alias('invoice_id'),col('date_creation').alias('date'),col('ref_customer').alias('customer_id'), col('totalpaid').alias('total_paid')).withColumn('branch_id',lit(branch_id))
                db_loader.write_to_table(transcations, 'client.transactions', math.ceil(transcations.count()/df_rows))

            elif i == 'sales':

                if product_prices == None:
                    
                    product_prices = dolibarr_products.select(col('date_modification').alias('date'),col('id').alias('product_id'),col('price').alias('selling_price'),'cost_price',col('fk_unit').alias('stock_id')).withColumn('sid',concat_ws('','date','product_id')).withColumn('branch_id',lit(branch_id))
                    product_prices = product_prices.select('sid','date','product_id','selling_price','cost_price','stock_id','branch_id')
                ## Sales table -> append
                ##need to add if statement --> if an offer id exists, then set item_id to null and place the offer id .. for later 
                ##for now i took socid as if its the offer_id :) 
                sales = dolibarr_invoices.select('id','date_creation','id',explode(dolibarr_invoices.lines).alias('product'))\
                .select(col('date_creation').alias('date'),col('id').alias('invoice_id'),col('product.product_ref').alias('product_id'),col('product.price').alias('sold_price'),col('product.qty').alias('quantity')).withColumn('branch_id',lit(branch_id))
                sales = sales.select('date','invoice_id','product_id','sold_price','quantity','branch_id')

                sales = sales.join(product_prices.select('product_id','sid'),sales.product_id == product_prices.product_id,"inner")
                sales = sales.select('date','invoice_id',col('sid').alias('item_id'),'sold_price','quantity','branch_id').withColumn('sid',concat_ws('','invoice_id','item_id')).withColumn('offer_id',lit(''))
                sales = sales.select('sid','date','invoice_id','item_id','offer_id','sold_price','quantity','branch_id')

                sales_count = sales.count()
                db_loader.write_to_table(sales, 'client.sales', math.ceil(sales_count/df_rows))
            else :
                continue
    
    except:
        print('Mapping data from file to database failed.')