from invoke import DolibarrInvoker
from pathlib import Path
import os
import sys
import datetime

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs as configs

di = DolibarrInvoker()
today = datetime.datetime.now().strftime('%Y-%m-%d')

def get_model(model_name, filters):

    global di
    limit = configs.dolibarr['limit']

    pagination = di.query(model = model_name, sqlfilters = filters, page = 1 , limit = limit, pagination_data = True)

    for page in range (1, pagination):
        di.query(model = model_name, sqlfilters = filters, page = page , limit = limit, pagination_data = True)    

# product collector

model_name = 'products'
filters = f'(tms:like:\'{today}%\')'
get_model(model_name)


# orders collector 
model_name = 'orders'
filters = f'(datec:like:\'{today}%\')'
get_model(model_name)

# stockmovements collector

model_name = 'stockmovements'
filters = f'(tms:like:\'{today}%\')'
get_model(model_name)

# invoices collector

model_name = 'invoices'
filters = f'(datec:like:\'{today}%\')'
get_model(model_name)