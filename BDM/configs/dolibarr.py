dolibarr = [
    {
        "model":"products",
        "date_filter":{
            "apply" : True,
            "field_name":"t.tms",
            "operator": "like"
        },
        "other_filters": None
    },
    {
        "model":"orders",
        "date_filter":{
            "apply" : True,
            "field_name":"t.datec",
            "operator": "like"
        },
        "other_filters": None
    },
    {
        "model":"stockmovements",
        "date_filter":{
            "apply" : True,
            "field_name":"tms",
            "operator": "like"
        },
        "other_filters": None
    },
    {
        "model":"invoices",
        "date_filter":{
            "apply" : True,
            "field_name":"datec",
            "operator": "like"
        },
        "other_filters": None
    }
]