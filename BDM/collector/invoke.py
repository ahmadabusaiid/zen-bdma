#!/usr/bin/env python

import xmlrpc.client
import json
from abc import abstractmethod
from dolibarr import Dolibarr
import requests
import datetime
import os
import errno
from pathlib import Path
import sys
 
sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs as configs


def mkdirs (path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

class Invoker:

    def __init__(self, uri, username, password, datasource):
        self._uri = uri
        self._username = username
        self._password = password
        output_dir = configs.global_params['temp_dir']
        self._datasource = f'{Path.home()}/{output_dir}/{datasource}'
        mkdirs(self._datasource)

    @abstractmethod
    def query(self):
        pass

class DolibarrInvoker(Invoker):

    def __init__(self):
        host_path = configs.dolibarr["host_path"]
        self._dolibarr_inst = Dolibarr(f'http://{host_path}/api/index.php/'.format(server=configs.dolibarr["host_path"]), configs.dolibarr["api_key"])

        super().__init__(None, None,None, configs.dolibarr['datasource_name'])

    def query(self, model, sqlfilters, page = 0, limit = 1000, pagination_data = True): 

        result = self._dolibarr_inst.call_list_api(model, { "page": page, "limit":limit, "pagination_data": pagination_data})
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d')

        odir_path = f'{self._datasource}/{model}/{timestamp}'
        mkdirs(odir_path)

        with open(f'{odir_path}/{page}.json', 'w') as f:

            if type(result) == 'dict':
                json.dump(result['data'], f, indent = 4)
                return result['pagination']
            else: 
                json.dump(result, f, indent=4)
                return 1
        

class OdooInvoker(Invoker):

    def __init__(self):

        info = xmlrpc.client.ServerProxy(configs.odoo['server_url']).start()
        url, db, username, password = info['host'], info['database'], info['user'], info['password']

        common = xmlrpc.client.ServerProxy('{url}/xmlrpc/2/common'.format(url = url))
        common.version()
        uid = common.authenticate(db, username, password, {})

        super().__init__(url, uid, password, configs.odoo['datasource_name'])
        self._db = db

    def query(self, model, action, filter, features, limit = 1000, offset=0):

        models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(self._uri))

        if action == 'search_read' : 
            result = models.execute_kw(
                self._db, 
                self._username, 
                self._password, 
                model, # model
                action,
                [filter], # filter
                {'fields': features, 'offset': offset, 'limit': limit} # features, limit criterias etc.
            )

            timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
            odir_path = f'{self._datasource}/{model}/{timestamp}'
            mkdirs(odir_path)

            with open(f'{odir_path}/{offset}.json', 'w') as f:
                json.dump(result, f, indent = 4)

        else:   ## for search_count api

            result = models.execute_kw(
                self._db, 
                self._username, 
                self._password, 
                model, # model
                action,
                [filter], # filter
            )
            return result

class WeatherAPIInvoker(Invoker):
    
    def __init__(self):

        super().__init__(configs.weather_api["server_url"], None, configs.weather_api["api_key"], configs.weather_api['datasource_name'])


    def query(self, path, ext_params):

        int_params = { "key" : self._password }
        params = {key: value for (key, value) in (int_params.items() | ext_params.items())}
        result = requests.get('{host_url}/{path}'.format(host_url = self._uri, path = path), params = params)

        timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
        
        file = path.split('.')
        odir_path = f'{self._datasource}/{file[0]}/{timestamp}'
        mkdirs(odir_path)
        with open(f'{odir_path}/1.json', 'w') as f:
            json.dump(result.json(), f, indent = 4)

