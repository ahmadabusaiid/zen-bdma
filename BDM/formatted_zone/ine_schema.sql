CREATE SCHEMA demographics;
t
CREATE TABLE population_details (
    sid string PRIMARY KEY, 
    cod string NOT NULL,
    year string NOT NULL, 
    date integer NOT NULL, 
    region string NOT NULL, 
    type string NOT NULL, 
    population_count float NOT NULL,  

)