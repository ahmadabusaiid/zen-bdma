import xmlrpc.client
import json
from abc import abstractmethod
from dolibarr import Dolibarr

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
        super(Invoker, self).__init__()


class OdooInvoker(Invoker):

    def __init__(self):

        info = xmlrpc.client.ServerProxy('https://demo.odoo.com/start').start()
        url, db, username, password = info['host'], info['database'], info['user'], info['password']

        common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
        common.version()
        uid = common.authenticate(db, username, password, {})

        super().__init__(url, uid, password)
        self._db = db

    def query(self):

        models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(self._uri))
        result = models.execute_kw(self._db, self._username, self._password, 'res.partner', 'search_read',[[['is_company', '=', True]]], {'fields': ['name', 'country_id', 'comment'], 'limit': 5})

        with open('data.json', 'w') as f:
            json.dump(result, f,indent=4)


mode = OdooInvoker()
mode.query()