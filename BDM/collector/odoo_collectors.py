from invoke import OdooInvoker
from pathlib import Path
import os
import sys
import datetime
 
sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs as configs

oi = OdooInvoker()
today = datetime.datetime.now().strftime('%Y-%m-%d')

def get_model(model, filters, features):

    global oi
    limit = configs.odoo['limit'] ## common limit for pagination

    count = oi.query(model = model, filter = filters, action = 'search_count', features = features)
    for offset in range (0, count, limit):
        oi.query(model = model, filter = filters, action = 'search_read', features = features, limit = limit, offset = offset)


# inventory stock collector

model_name = 'stock.report'

filters = [
    ['state', '=', 'confirmed'],
    ['creation_date','like',f'{today}%'] ## '%Y-%m-%d %H:%M:%s'
]
features = [
    'company_id',
    'display_name',
    'categ_id',  
    'creation_date', 
    'cycle_time', 
    'date_done',
    'delay',
    'is_backorder',
    'is_late',
    'partner_id',
    'picking_id',
    'product_id',
    'product_qty',
    'scheduled_date',
    'stock_value'
]
get_model(model_name, filters, features)

# Invoice collector

model_name = 'account.invoice.report'
filters = [
    ['state', '=', 'posted'],
    ['invoice_date','=',f'{today}'] ## '%Y-%m-%d
]
features = [
    'company_id',
    'country_id',
    'account_id', 
    'invoice_date', 
    'invoice_user_id',
    'product_id',
    'product_categ_id',  
    'quantity',
    'price_subtotal',
    'price_total',
    'partner_id'
]
get_model(model_name, filters, features)