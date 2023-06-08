CREATE SCHEMA demographics;

CREATE TABLE demographics.population_details (
    sid string PRIMARY KEY,
    for_year integer NOT NULL, 
    date date NOT NULL,
    cod string NOT NULL,
    region string NOT NULL, 
    segment string NOT NULL, 
    population_count float NOT NULL
);