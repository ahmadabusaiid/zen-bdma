#!/usr/bin/env python

global_params = {
    "temp_dir" : "data",
    "collectors": ["ineapi"],
    "loader" : "hdfs_loader",
    "branch_id":"BCN"
}
    
## collector 
dolibarr = {
    "datasource_name": "dolibarr",
    "host_path" : '10.4.41.57:80',
    "api_key" : "nd6hgbcr",
    "limit": 1000
}

odoo = {
    "datasource_name": "odoo",
    "server_url" : "https://demo.odoo.com/start",
    "limit": 1000
}

weather_api = {
    "datasource_name": "weatherapi",
    "server_url" : 'http://api.weatherapi.com/v1',
    "api_key" : "e3cf29be53a84b4d961224304231204",
    "forecast":{
        "city" : "Barcelona",
        "days": 1
    },
    "history":{
        "start_date": "2020-01-01",
        "end_date" :"2020-01-02"
    }

}

ine_api = {
    "datasource_name": "ine",
    "server_url" : "https://servicios.ine.es/wstempus/js/ES",
    "provincial": {
        "start_date": "2014-01-01",
        "end_date" :"2018-01-01"      
    }
}

## persistent loader

hdfs = {
    "host_path" : '10.4.41.57:27000/user/bdm'
}

##spark

spark = {
    "appName" : "formatter"
}

##formatted_zone_storage

monetdb = {
    "host_path": "monetdb://localhost:50000",
    "database" :"monetdb",
    "user" :"monetdb",
    "password" :"bdm",
    "driver" : "org.monetdb.jdbc.MonetDriver"
    "driver_path" : "/home/bdm/BDM_Software/spark/jars/monetdb-jdbc-3.3.jre8.jar"
}
