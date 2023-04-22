# BDM Project 

## P1: Landing Zone 

#### Requirement to run the Python code:

##### Collectors

1. Having access to Dolibarr endpoint. 
2. Installation of Dolibarr and Odoo python libraries. 

##### Presistent Loaders

1. Installation and setup of HDFS. 

#### How to setup configurations

##### In the configs folder:
1. common.py:
* Configure the api endpoints and keys.

| Data source | Endpoint |
| ------ | ------ |
| Odoo | Demo application with public access to their api|
| Dolibarr | Application was setup and installed on UPC Virtual Machine|
| Weather api | Free public access to their api |

* limit of records retrieved from the data sources. 
 
 | Data source | Limit |
| ------ | ------ |
| Odoo | 1000 records|
| Dolibarr | 1000 records|
| Weather api | no specific record limit can be set |
 
* Configure presistent landing zone endpoint. 
-- HDFS on UPC virtual machine. 

2. odoo.py, dolibarr.py, weatherapi.py:
* Configure modules called per api. 




