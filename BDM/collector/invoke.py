#!/usr/bin/env python

import xmlrpc.client
import json
from abc import abstractmethod
from dolibarr import Dolibarr
import configs
<<<<<<< Updated upstream
=======
#import request
>>>>>>> Stashed changes
import requests

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
        host_path = configs.dolibarr["host_path"]
        self._dolibarr_inst = Dolibarr(f'http://{host_path}/api/index.php/'.format(server=configs.dolibarr["host_path"]), configs.dolibarr["api_key"])

<<<<<<< Updated upstream
        dolibarr_inst = Dolibarr('http://{server}:{port}/api/index.php/'.format(server=configs.dolibarr["server"], port=configs.dolibarr["port"]), configs.dolibarr["api_key"])
        super().__init__(dolibarr_inst, None, None)
    
    def query(self, model):

        product_dict = self._uri.call_list_api(model) #ex: 'products'
        with open('data_dolibarr.json'.format(), 'w') as f:
            json.dump(product_dict, f,indent=4)

=======
        super(Invoker, self).__init__()
    def query(self, model): 
        result = self._dolibarr_inst.call_list_api(model)
        with open(f'{model}.json', 'w') as f:
            json.dump(result, f, indent = 4)
>>>>>>> Stashed changes

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


    def query(self, path, ext_params):

        int_params = { "key" : self._password }
        params = {key: value for (key, value) in (int_params.items() | ext_params.items())}

        result = requests.get('{host_url}/{path}'.format(host_url = self._uri, path = path), params = params)

<<<<<<< Updated upstream
        file = path.split('.')
        with open(f'{file[0]}.json', 'w') as f:
            json.dump(result.json(), f, indent = 4)
=======
        
#WeatherAPIInvoker().query()
>>>>>>> Stashed changes

