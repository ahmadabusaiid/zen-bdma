from invoke import DolibarrInvoker

oi = DolibarrInvoker()

# product collector

model_name = 'products'

oi.query(model = model_name)

# orders collector 
model_name = 'orders'

oi.query(model = model_name)

# stockmovements collector

model_name = 'stockmovements'

oi.query(model = model_name)

# invoices collector

model_name = 'invoices'

oi.query(model = model_name)
