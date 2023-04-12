from invoke import OdooInvoker

oi = OdooInvoker()

# inventory stock collector

# model_name = 'stock.report'
# filters = [
#     ['state', '=', 'confirmed']
# ]
# features = [
#     'company_id',
#     'display_name',
#     'categ_id',  
#     'creation_date', 
#     'cycle_time', 
#     'date_done',
#     'delay',
#     'is_backorder',
#     'is_late',
#     'partner_id',
#     'picking_id',
#     'product_id',
#     'product_qty',
#     'scheduled_date',
#     'stock_value'
# ]
# limit = 1000

# oi.query(model = model_name, filter = filters, features = features, limit = limit)



# POS collector

model_name = 'pos.order.line'
filters = [
    ['price_subtotal', '>', 0]
]
features = [
    'company_id',
    'create_date',
    'currency_id' 
    # 'invoiced', 
    # 'partner_id', 
    # 'pos_categ_id',
    # 'total_discount',
    # 'price_sub_total',
    # 'price_total',
    # 'product_categ_id',
    # 'product_id',
    # 'product_qty',
    # 'session_id'
]
limit = 1000

oi.query(model = model_name, filter = filters, features = features, limit = limit)