from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
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

productSchema = StructType([
    StructField("product_id",StringType(),True),
    StructField("label",StringType(),True),
    StructField("description",StringType(),False),
    StructField("type", StringType(), True),
    StructField("branch_id", StringType(), True)
  ])

customerSchema = StructType([
    StructField("customer_id",StringType(),True),
    StructField("first_name",StringType(),True),
    StructField("last_name",StringType(),True),
    StructField("branch_id", StringType(), True)
  ])

def update_products(product):
    
    if product['product_id'] == None :
        product['product_id'] = product['ex_product_id']
        product['label'] = product['ex_label']
        product['description'] = product['ex_description']
        product['type'] = product['ex_type']
        product['branch_id'] = product['ex_branch_id']
    
    return (product['product_id'],product['label'],product['description'],product['type'],product['branch_id'])

def update_customer_details(customer):
    
    if customer['customer_id'] == None :

        customer['customer_id'] = product['ex_customer_id']
        customer['first_name'] = customer['ex_first_name']
        customer['last_name'] = customer['ex_last_name']
        customer['branch_id'] = customer['ex_branch_id']
    
    return (customer['customer_id'],customer['first_name'],customer['last_name'],customer['branch_id'])

def map_to_db(today, branch_id):

    try : 
        db_loader = DBLoader()
        driver_path = db_loader.get_driver_path()

        hdfs_host = common.hdfs['host_path']

        conf = SparkConf()
        conf.set("spark.jars", driver_path)
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        spark = SparkSession.builder.config(conf=conf).master("local").appName(common.spark['appName']).getOrCreate()

        dolibarr_invoices=spark.read.parquet(f'hdfs://{hdfs_host}/{branch_id}/dolibarr/invoices/{today}').cache()
        dolibarr_products=spark.read.parquet(f'hdfs://{hdfs_host}/{branch_id}/dolibarr/products/{today}').cache()
        dolibarr_stock_in=spark.read.parquet(f'hdfs://{hdfs_host}/{branch_id}/dolibarr/stockmovements/{today}').cache()
        dolibarr_inventory=spark.read.parquet(f'hdfs://{hdfs_host}/{branch_id}/dolibarr/inventory/{today}').cache()

        tables_to_load = common.global_params['load_tables']

        product_prices = None

        for i in tables_to_load:

            if i == 'products':

                ## Drop foreign key 
                db_loader.run_query( "ALTER TABLE IF EXISTS client.product_prices DROP CONSTRAINT product_prices_product_id_fkey;")
                db_loader.run_query("ALTER TABLE IF EXISTS client.shipments DROP CONSTRAINT shipments_product_id_fkey;")
                
                ## Products table -> update
                products = dolibarr_products.select(col('id').alias('product_id'),'label','description','type').distinct().withColumn('branch_id',lit(branch_id))
                ex_products = db_loader.read_table(spark, \
                    '''SELECT product_id as ex_product_id, label as ex_label, description as ex_description, type as ex_type, branch_id as ex_branch_id FROM client.products''').cache()
                j_products = products.join(ex_products, products.product_id ==  ex_products.ex_product_id, "outer")
                j_products_rdd = j_products.rdd.map(update_products)
                j_products_new = j_products_rdd.toDF(schema = productSchema)
                
                db_loader.write_to_table(j_products_new, 'client.products', math.ceil(j_products_new.count()/df_rows),'overwrite', True)

                db_loader.run_query('''ALTER TABLE client.products ADD CONSTRAINT products_product_id_pkey PRIMARY KEY ("product_id");''')
                db_loader.run_query('''ALTER TABLE client.product_prices ADD CONSTRAINT product_prices_product_id_fkey FOREIGN KEY ("product_id") REFERENCES client.products ("product_id");''')
                db_loader.run_query('''ALTER TABLE client.shipments ADD CONSTRAINT shipments_product_id_fkey FOREIGN KEY ("product_id") REFERENCES client.products ("product_id");''')
            
            elif i == 'shipments':

                ## Stocks table -> append
                shipments = dolibarr_stock_in.select(col('id').alias('shipment_id'),col('date_creation').alias('date'),col('product_id').alias('product_id'),col('qty').alias('quantity'),col('eatby').alias('expiry_date')).withColumn('branch_id',lit(branch_id))
                db_loader.write_to_table(shipments, 'client.shipments', math.ceil(shipments.count()/df_rows))
            
            elif i == 'inventory':


                ## Inventory table -> append
                inventory = dolibarr_inventory.select(col('stock_id').alias('shipment_id'),'date',col('qty').alias('quantity')).withColumn('branch_id',lit(branch_id))
                db_loader.write_to_table(inventory, 'client.inventory', math.ceil(inventory.count()/df_rows))
            
            elif i == 'product_prices' :

                ## Product prices table -> append
                if product_prices == None:
                    product_prices = dolibarr_products.select(col('date_modification').alias('date'),col('id').alias('product_id'),col('price').alias('selling_price'),'cost_price',col('fk_unit').alias('shipment_id')).withColumn('sid',concat_ws('','date','product_id')).withColumn('branch_id',lit(branch_id))
                    product_prices = product_prices.select('sid','date','product_id','selling_price','cost_price','shipment_id','branch_id')
                db_loader.write_to_table(product_prices, 'client.product_prices', math.ceil(product_prices.count()/df_rows))

            elif i == 'customers':

                ## Drop FKEY Constraints
                db_loader.run_query( "ALTER TABLE IF EXISTS client.transactions DROP CONSTRAINT transactions_customer_id_fkey;")

                ## Customers table -> update
                customer_details = dolibarr_invoices.select(col('ref_customer').alias('customer_id'),col('firstname').alias('first_name'),col('lastname').alias('last_name')).distinct().withColumn('branch_id',lit(branch_id))
                ex_customer_details = db_loader.read_table(spark, '''SELECT customer_id as ex_customer_id, first_name as ex_first_name, last_name as ex_last_name, branch_id as ex_branch_id FROM client.customers''').cache()

                j_customers = customer_details.join(ex_customer_details, customer_details.customer_id ==  ex_customer_details.ex_customer_id, "outer")
                j_customers_rdd = j_customers.rdd.map(update_customer_details)
                j_customers_new = j_customers_rdd.toDF(schema=customerSchema)

                db_loader.write_to_table(j_customers_new, 'client.customers', math.ceil(j_customers_new.count()/df_rows),'overwrite', True)

                db_loader.run_query('''ALTER TABLE client.customers ADD CONSTRAINT customers_customer_id_pkey PRIMARY KEY ("customer_id");''')
                db_loader.run_query('''ALTER TABLE client.transactions ADD CONSTRAINT transactions_customer_id_fkey FOREIGN KEY ("customer_id") REFERENCES client.customers ("customer_id");''')
            
            elif i == 'transactions':
                ## Transaction table -> append
                transcations = dolibarr_invoices.select(col('id').alias('invoice_id'),col('date_creation').alias('date'),col('ref_customer').alias('customer_id'), col('totalpaid').alias('total_paid')).withColumn('branch_id',lit(branch_id))
                db_loader.write_to_table(transcations, 'client.transactions', math.ceil(transcations.count()/df_rows))

            elif i == 'sales':

                if product_prices == None:
                    
                    product_prices = dolibarr_products.select(col('date_modification').alias('date'),col('id').alias('product_id'),col('price').alias('selling_price'),'cost_price',col('fk_unit').alias('shipment_id')).withColumn('sid',concat_ws('','date','product_id')).withColumn('branch_id',lit(branch_id))
                    product_prices = product_prices.select('sid','date','product_id','selling_price','cost_price','shipment_id','branch_id')
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