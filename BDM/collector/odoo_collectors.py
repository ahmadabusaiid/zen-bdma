from invoke import OdooInvoker

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
limit = 1000

oi.query(model = model_name, filter = filters, features = features, limit = limit)



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
limit = 1000

oi.query(model = model_name, filter = filters, features = features, limit = limit)