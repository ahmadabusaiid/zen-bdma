from collector.invoke import OdooInvoker
from pathlib import Path
import os
import sys
 
sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as configs

def get_model(oi, model, filters, features):

    limit = configs.odoo['limit'] ## common limit for pagination

    count = oi.query(model = model, filter = filters, action = 'search_count', features = features)     ## get the count of records after applying the query
    for offset in range (0, count, limit):
        oi.query(model = model, filter = filters, action = 'search_read', features = features, limit = limit, offset = offset)


def collect(odoo_configs, date): 

    oi = OdooInvoker()

    for query_model in odoo_configs:

        filters = query_model['other_filters']
        date_filter = query_model['date_filter']

        if date_filter['apply']:    ## add query by date or not

            if date_filter['operator'] == 'like':
                filters.append([date_filter['field_name'], date_filter['operator'], f'{date}%'])
            else :
                filters.append([date_filter['field_name'], date_filter['operator'], f'{date}'])

        get_model(oi, query_model['model'], filters ,query_model['features'])
