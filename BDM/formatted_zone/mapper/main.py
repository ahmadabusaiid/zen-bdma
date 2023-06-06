import configs.common as common_configs
from transformations import dolibarr
from transformations import ineapi
from transformations import weatherapi

import datetime

today = datetime.datetime.now().strftime('%Y-%m-%d')
branch_name = common_configs.global_params['branch_id']

print("================== Initiate : Formatted Zone Loading ==================")

for collector_name in common_configs.global_params['load_sources']:

    if collector_name == 'dolibarr':
        dolibarr.map_to_db(today, branch_name)
    if collector_name == 'weatherapi':
        weatherapi.map_to_db(today)
    if collector_name == 'ineapi':
        ineapi.map_to_db(today)

print("================== Done : Formatted Zone Loading ==================")
