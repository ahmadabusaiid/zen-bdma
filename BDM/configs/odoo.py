odoo = [
    {
        "model": 'stock.report',
        "date_filter":{
            "apply" : True,
            "field_name":"creation_date",
            "operator": "like"
        },
        "other_filters": [
            ['state', '=', 'confirmed']
        ],
        "features" : [
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
    },
    {
        "model": 'account.invoice.report',
        "date_filter":{
            "apply" : True,
            "field_name":"invoice_date",
            "operator": "="
        },
        "other_filters": [
            ['state', '=', 'posted']
        ],
        "features" : [
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
    }
]