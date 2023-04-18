from invoke import OdooInvoker
from pathlib import Path
import os
import sys
 
sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs as configs

oi = OdooInvoker()

# inventory stock collector

model_name = 'stock.report'
filters = [
    ['state', '=', 'confirmed']
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

limit = configs.odoo['limit']
count = oi.query(model = model_name, filter = filters, action = 'search_count', features = features)
for offset in range (0, count, limit):
    oi.query(model = model_name, filter = filters, action = 'search_read', features = features, limit = limit, offset = offset)



# Invoice collector

model_name = 'account.invoice.report'
filters = [
    ['state', '=', 'posted']
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

limit = configs.odoo['limit']
count = oi.query(model = model_name, filter = filters, action = 'search_count', features = features)
for offset in range (0, count, limit):
    oi.query(model = model_name, filter = filters, action = 'search_read', features = features, limit = limit, offset = offset)