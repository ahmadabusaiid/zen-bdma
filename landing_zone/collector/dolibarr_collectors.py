from collector.invoke import DolibarrInvoker
from pathlib import Path
import os
import sys

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as common

def get_model(di, model_name, filters):

    limit = common.dolibarr['limit']

    pagination = di.query(model = model_name, sqlfilters = filters, page = 0 , limit = limit, pagination_data = True)

    for page in range (1, pagination['page_count']):
        di.query(model = model_name, sqlfilters = filters, page = page , limit = limit, pagination_data = True)    

def collect(dolibarr_configs, date):

    di = DolibarrInvoker()

    for query_model in dolibarr_configs:

        filters = ''
        if query_model['other_filters'] != None and query_model['other_filters'] != '':
            filters = query_model['other_filters']
        
        date_filter = query_model['date_filter']

        if date_filter['apply']:    ## add query by date or not

            field = date_filter['field_name']
            operator = date_filter['operator']

            if date_filter['operator'] == 'like':
                date_sqlfilter = f'({field}:{operator}:\'{date}%\')'
            else :
                date_sqlfilter = f'({field}:{operator}:\'{date}\')'

            if filters == '':
                filters = date_sqlfilter
            else:
                filters = f'{filters} and {date_sqlfilter}'

        get_model(di, query_model['model'], filters)