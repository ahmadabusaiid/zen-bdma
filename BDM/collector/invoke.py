#!/usr/bin/env python

import xmlrpc.client
import json
from abc import abstractmethod
from dolibarr import Dolibarr
import configs
import request

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

        dolibarr_inst = Dolibarr('http://{host_path}/api/index.php/'.format(server=configs.dolibarr["host_path"]), configs.dolibarr["api_key"])
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

    def query(self, model, filter, features, limit):

        models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(self._uri))
        result = models.execute_kw(
            self._db, 
            self._username, 
            self._password, 
            model, # model
            'search_read',
            [filter], # filter
            {'fields': features, 'limit': limit} # features, limit criterias etc.
        )

        with open(f'{model}.json', 'w') as f:
            json.dump(result, f, indent = 4)

class WeatherAPIInvoker(Invoker):
    
    def __init__(self):

        super().__init__(configs.weather_api["server_url"], None, configs.weather_api["api_key"])

    def query(self):

        result = requests.get('{}/forecast'.format(self._uri), params={ key : self._password, q: "Barcelona", days : 1 })

        with open('weather.json', 'w') as f:
            json.dump(result, f, indent = 4)


        
WeatherAPIInvoker().query()

