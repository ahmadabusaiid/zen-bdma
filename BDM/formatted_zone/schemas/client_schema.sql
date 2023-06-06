
-- ************************TABLE********************************

CREATE SCHEMA client; 

CREATE TABLE client.products(
	product_id string PRIMARY KEY,
	label string NOT NULL,
	description string,
	type string NOT NULL,
	branch_id string NOT NULL
);

CREATE TABLE client.stocks(
	stock_id string PRIMARY KEY,
	date date NOT NULL,
	product_id string NOT NULL,
	quantity integer NOT NULL,
	expiry_date date NOT NULL,
	branch_id string NOT NULL,
	FOREIGN KEY(product_id) REFERENCES client.products(product_id)
);

CREATE TABLE client.inventory(
	stock_id string NOT NULL,
	date date NOT NULL,
	quantity integer NOT NULL,
	branch_id string NOT NULL,
	PRIMARY KEY(stock_id, date)
);

CREATE TABLE client.offers(
	offer_id string PRIMARY KEY,
	date date NOT NULL,
	discount_price float NOT NULL,
	original_price float NOT NULL,
	branch_id string NOT NULL
);

CREATE TABLE client.product_prices(
	sid string PRIMARY KEY,
	date date NOT NULL,
	product_id string NOT NULL,
	selling_price float NOT NULL,
	cost_price float NOT NULL,
	stock_id string NOT NULL,
	branch_id string NOT NULL,
	FOREIGN KEY(product_id) REFERENCES products(product_id),
	FOREIGN KEY(stock_id) REFERENCES stocks(stock_id)
);

CREATE TABLE client.offer_details(
	offer_id string NOT NULL,
	item_id string NOT NULL,
	quantity integer NOT NULL,
	branch_id string NOT NULL,
	PRIMARY KEY(offer_id, item_id)
);

CREATE TABLE client.customers(
	customer_id string PRIMARY KEY,
	first_name string NOT NULL,
	last_name string NOT NULL,
	branch_id string NOT NULL
);

CREATE TABLE client.transactions(
	invoice_id string PRIMARY KEY,
	date date NOT NULL,
	customer_id string NOT NULL,
	total_paid float NOT NULL,
	branch_id string NOT NULL,
	FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE client.sales(
	sid string PRIMARY KEY,
	date date NOT NULL,
	invoice_id string NOT NULL,
	item_id string,
	offer_id string,
	sold_price float NOT NULL,
	quantity integer NOT NULL,
	branch_id string NOT NULL,
	FOREIGN KEY(item_id) REFERENCES product_prices(sid),
	FOREIGN KEY(invoice_id) REFERENCES transactions(invoice_id)
);

-- ************************INDEX********************************

drop index stock_in_date;
drop index inventory_date;
drop index product_pricing_date;
drop index transaction_date;
drop index sales_date;

create index stock_in_date on client.stocks(date);
create index inventory_date on client.inventory(date);
create index product_pricing_date on client.product_prices(date);
create index transaction_date on client.transactions(date);
create index sales_date on client.sales(date);

----------------------------------------------------------------