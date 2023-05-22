from collector import odoo_collectors
from collector import dolibarr_collectors
from collector import weather_collectors
from collector import ine_collectors

import configs.odoo as odoo_configs
import configs.dolibarr as dolibarr_configs
import configs.common as common_configs
import configs.weatherapi as weatherapi_configs
import configs.ine as ineapi_configs

from persistent_loader import hdfs_loader
import datetime

today = datetime.datetime.now().strftime('%Y-%m-%d')

print("================== Initiate : Temporary Landing Zone ==================")

for collector_name in common_configs.global_params['collectors']:

    if collector_name == 'odoo':
        odoo_collectors.collect(odoo_configs, today)
    if collector_name == 'dolibarr':
        dolibarr_collectors.collect(dolibarr_configs, today)
    if collector_name == 'weatherapi':
        weather_collectors.collect()
    if collector_name == 'ineapi':
        ine_collectors.collect()

print("================== Done : Temporary Landing Zone ==================")

print("================== Initiate : Persistent Landing Zone ==================")
if common_configs.global_params['loader'] == 'hdfs_loader':

    hdfs = hdfs_loader.HDFSLoader()

    for collector_name in common_configs.global_params['collectors']:

        if collector_name == 'odoo':
            collector_config = odoo_configs

        if collector_name == 'dolibarr':
            collector_config = dolibarr_configs

        if collector_name == 'weatherapi':
            collector_config = weatherapi_configs
        
        if collector_name == 'ineapi':
            collector_config = ineapi_configs

        for model in collector_config:
            model_name = model['model']
            path = f'{collector_name}/{model_name}/{today}'
            hdfs.write(path)

print("================== Initiate : Persistent Landing Zone ==================")