#!/usr/bin/env python

global_params = {
    "temp_dir" : "data",
    "branch_id":"BCN",
    "load_sources" :['dolibarr','ineapi','weatherapi']
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
    "host_path": "monetdb://localhost:50001",
    "database" :"monetdb",
    "user" :"monetdb",
    "password" :"bdm",
    "driver" : "org.monetdb.jdbc.MonetDriver",
    "driver_path" : "/home/bdm/BDM_Software/spark/jars/monetdb-jdbc-3.3.jre8.jar"
}
