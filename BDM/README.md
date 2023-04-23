# BDM Project 

## P1: Landing Zone 

### Pipeline Reproducibility Steps

1. **Requirements:**

    1. Collectors
        -   Having access to the Dolibarr endpoint. 
        -   Installation of Dolibarr and Odoo python libraries. 

    2. Persistent Loaders
        - Having access to the HDFS endpoint. 

2. **How to setup configurations**
    - In the configs folder:
        1. /common.py:
        
            - Configure the api endpoints and keys


            | Data source | Endpoint |
            | ------ | ------ |
            | Odoo | Demo application with public access to their api|
            | Dolibarr | Application was setup and installed on UPC Virtual Machine|
            | Weather api | Free public access to their api |

            - Limit of records retrieved from the data sources


            | Data source | Limit |
            | ------ | ------ |
            | Odoo | 1000 records|
            | Dolibarr | 1000 records|
            | Weather api | no specific record limit can be set |
 
            - Configure the persistent landing zone endpoint
                - HDFS on the UPC virtual machine

        2. Configure the data models, fields and other parameters for each data source
            - odoo.py
            - dolibarr.py
            - weatherapi.py

3. **Run the pipeline:**
    1. Clone the project to your desired directory
    2. pip install -r ./requirements.txt
    3. Run main.py