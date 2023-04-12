#!/usr/bin/env python

import xmlrpc.client
import json
from abc import abstractmethod
from dolibarr import Dolibarr
import configs

class Invoker:

    def __init__(self, uri, username, password):
        self._uri = uri
        self._username = username
        self._password = password

    @abstractmethod
    def query(self):
        pass

class DolibarrInvoker(Invoker):

    def __init__(self):

        dolibarr_inst = Dolibarr('http://{server}:{port}/api/index.php/'.format(server=configs.dolibarr["server"], port=configs.dolibarr["port"]), configs.dolibarr["api_key"])
        product_dict = dolibarr_inst.call_list_api('products')
        print(product_dict)
        super(Invoker, self).__init__()


class OdooInvoker(Invoker):

    def __init__(self):

        info = xmlrpc.client.ServerProxy(configs.odoo['server_url']).start()
        url, db, username, password = info['host'], info['database'], info['user'], info['password']

        common = xmlrpc.client.ServerProxy('{url}/xmlrpc/2/common'.format(url = url))
        common.version()
        uid = common.authenticate(db, username, password, {})

        super().__init__(url, uid, password)
        self._db = db

    def query(self, model, filter, projection):

        models = xmlrpc.client.ServerProxy('{url}/xmlrpc/2/object'.format(url = self._uri))
        result = models.execute_kw(self._db, self._username, self._password, model, 'search_read',[[filter]], projection )

        with open('data.json', 'w') as f:
            json.dump(result, f,indent=4)

# mode = DolibarrInvoker()
mode = OdooInvoker()
mode.query('report.stock.quantity', ['state', '!=', 'forecast'],{'fields': ['id', 'company_id', 'date', 'display_name','product_id','product_qty','state'], 'limit': 5})