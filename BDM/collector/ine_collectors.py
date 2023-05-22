from collector.invoke import INEAPIInvoker
import os
from pathlib import Path
import sys
 

## Website https://www.ine.es/dyngs/DataLab/manual.html?cid=66

sys.path.insert(0, os.path.dirname(Path(__file__).parent.absolute()))
import configs.common as configs

def collect():
    inei = INEAPIInvoker()

    start_date = configs.ine_api["provincial"]["start_date"].replace("-","")
    end_date = configs.ine_api["provincial"]["end_date"].replace("-","")

    inei.query(path = 'DATOS_TABLA/9687', ext_params = { "date": f'{start_date}:{end_date}'}, file='population')