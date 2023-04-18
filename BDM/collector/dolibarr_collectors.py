from invoke import DolibarrInvoker
from pathlib import Path
import os
import sys
 
sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs as configs

di = DolibarrInvoker()


def get_model(model_name):

    global di
    limit = configs.dolibarr['limit']

    pagination = di.query(model = model_name, page = 1 , limit = limit, pagination_data = True)

    for page in range (1, pagination):
        di.query(model = model_name, page = page , limit = limit, pagination_data = True)    

# product collector

model_name = 'products'
get_model(model_name)


# orders collector 
model_name = 'orders'
get_model(model_name)

# stockmovements collector

model_name = 'stockmovements'
get_model(model_name)

# invoices collector

model_name = 'invoices'
get_model(model_name)