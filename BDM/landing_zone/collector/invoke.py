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
import configs.common as common


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
        output_dir = common.global_params['temp_dir']
        branch_id = common.global_params['branch_id']
        self._datasource = f'{Path.home()}/{output_dir}/{branch_id}/{datasource}'
        mkdirs(self._datasource)

    @abstractmethod
    def query(self):
        pass

class DolibarrInvoker(Invoker):

    def __init__(self):
        host_path = common.dolibarr["host_path"]
        self._dolibarr_inst = Dolibarr(f'http://{host_path}/api/index.php/'.format(server=common.dolibarr["host_path"]), common.dolibarr["api_key"])

        super().__init__(None, None,None, common.dolibarr['datasource_name'])

    def query(self, model, sqlfilters='', page = 0, limit = 1000, pagination_data = True): 

        params = { "page": page, "limit":limit, "pagination_data": pagination_data }

        if sqlfilters != '':
            params['sqlfilters'] = sqlfilters
        
        print(params)
        result = self._dolibarr_inst.call_list_api(model, params)
        if "error" in result:
            error = result['error']
            print (f'Error : invoker = Dolibarr, error_message = {error}')
            return {"page_count":1}
        
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
        odir_path = f'{self._datasource}/{model}/{timestamp}'
        mkdirs(odir_path)

        with open(f'{odir_path}/{page}.json', 'w') as f:

            if "data" in result:
                json.dump(result['data'], f, indent = 4)
                return result['pagination']
            else: 
                json.dump(result, f, indent=4)
                return {"page_count":1}
        

class OdooInvoker(Invoker):

    def __init__(self):

        info = xmlrpc.client.ServerProxy(common.odoo['server_url']).start()
        url, db, username, password = info['host'], info['database'], info['user'], info['password']

        comm = xmlrpc.client.ServerProxy('{url}/xmlrpc/2/common'.format(url = url))
        comm.version()
        uid = comm.authenticate(db, username, password, {})

        super().__init__(url, uid, password, common.odoo['datasource_name'])
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

            if not result:
                print (f'Info : invoker = Odoo, info = No results')

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

        super().__init__(common.weather_api["server_url"], None, common.weather_api["api_key"], common.weather_api['datasource_name'])


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

class INEAPIInvoker(Invoker):
    
    def __init__(self):
        super().__init__(common.ine_api['server_url'], None, None, common.ine_api['datasource_name'])

    def query(self, path, file, ext_params):
        
        result = requests.get('{server_url}/{path}'.format(server_url = self._uri, path = path), params = ext_params)

        timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
        
        odir_path = f'{self._datasource}/{file}/{timestamp}'
        mkdirs(odir_path)
        with open(f'{odir_path}/1.json', 'w') as f:
            json.dump(result.json(), f, indent = 4)
