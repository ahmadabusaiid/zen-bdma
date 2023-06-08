SET schema client;

DROP TABLE IF EXISTS client.sales;
DROP TABLE IF EXISTS client.inventory;
DROP TABLE IF EXISTS client.offers;
DROP TABLE IF EXISTS client.offer_details;
DROP TABLE IF EXISTS client.transactions;
DROP TABLE IF EXISTS client.customers;
DROP TABLE IF EXISTS client.product_prices;
DROP TABLE IF EXISTS client.shipments;
DROP TABLE IF EXISTS client.products;

drop index client.stock_in_date;
drop index client.inventory_date;
drop index client.product_pricing_date;
drop index client.transaction_date;
drop index client.sales_date;

SET schema demographics;

DROP TABLE IF EXISTS demographics.population_details;

SET SCHEMA weather; 

DROP TABLE IF EXISTS weather.forecast;