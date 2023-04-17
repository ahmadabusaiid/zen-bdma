#!/usr/bin/env python

## collector 
dolibarr = {
    "datasource_name": "dolibarr",
    "host_path" : '10.4.41.57:80',
    "api_key" : "nd6hgbcr"
}

odoo = {
    "datasource_name": "odoo",
    "server_url" : "https://demo.odoo.com/start"
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

## persistent loader

hdfs = {
    "host_path" : '10.4.41.57:9870'
}