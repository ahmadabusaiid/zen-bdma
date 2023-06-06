-- ************************VIEWS********************************

----------------------------------------------------------------
-- View 1: Calculate average shelf life
----------------------------------------------------------------
drop view if exists client.avg_shelf_life;
create view client.avg_shelf_life as
	with base as (
		select
			  p.product_id
			, p.label
			, p.type
			, s.stock_id
			, s.date
			, s.expiry_date
			,"day"((s.expiry_date - s.date)) as shelf_life
			, s.branch_id
		from client.products p
		inner join client.stocks s
			on p.product_id = s.product_id
		where s.in_shelf = true
	)

	, agg1 as (
		select
			  type as product_type
			, avg(shelf_life) as avg_shelf_life
		from base
		group by type
		order by avg_shelf_life
	)

	select 
	*
	from agg1;



----------------------------------------------------------------
-- View 2: Calculate stocks remaining
----------------------------------------------------------------
drop view if exists client.remaining_stock;
create view client.remaining_stock as
	with base as (
		select
			  s.stock_id
			, s.date as stock_in
			, s.product_id
			, p.label
			, s.branch_id
			, i.date as inv_date
			, i.quantity
			, row_number() over(partition by s.stock_id order by i.date desc) as rn
		from client.stocks s
		inner join client.inventory i
			on s.stock_id = i.stock_id
		inner join client.products p
			on s.product_id = p.product_id
	)

	select 
		  stock_id
		, product_id
		, label as product_label
		, quantity as stock_remaining
		, branch_id
	from base
	where rn = 1
	order by stock_id;



----------------------------------------------------------------
-- View 3: Calculated stock waste and revenue loss
----------------------------------------------------------------
drop view if exists client.stock_waste;
create view client.stock_waste as
	with base as (
		select
			  s.stock_id
			, s.date as stock_in
			, s.expiry_date
			, s.product_id
			, s.branch_id
			, ((s.expiry_date - s.date)/(60*60*24)) as shelf_life
			, s.quantity as original_qty
			, i.quantity
			, row_number() over(partition by s.stock_id order by i.date) as rn
		from client.stocks s
		left join client.inventory i
			on s.stock_id = i.stock_id
			and i.date >= s.expiry_date
	)

	, summary as (
		select 
			  b.stock_id
			, stock_in
			, expiry_date
			, b.product_id
			, p.label as product_label
			, p.type as product_type
			, shelf_life as shelf_life
			, original_qty
			, coalesce(quantity, 0) as stock_wasted
			, coalesce(quantity, 0) / cast(original_qty as float) as proportion_wasted
			, pp.selling_price
			, pp.selling_price * coalesce(quantity, 0) as revenue_lost
			, b.branch_id
		from base b
		inner join client.products p
			on b.product_id = p.product_id
		inner join client.product_prices pp
			on b.product_id = pp.product_id
			and b.stock_id = pp.stock_id
		where rn = 1
		order by stock_id
	)

	select
	*
	from summary;



----------------------------------------------------------------
-- View 4: Calculate how close products are to expiry
----------------------------------------------------------------
drop view if exists client.duration_to_expiry;
create view client.duration_to_expiry as
	with base as (
		select
			  s.stock_id
			, s.date as stock_in
			, s.product_id
			, p.label
			, s.expiry_date
			, s.branch_id
			, i.date as inv_date
			, i.quantity
			, row_number() over(partition by s.stock_id order by i.date desc) as rn
		from client.stocks s
		inner join client.inventory i
			on s.stock_id = i.stock_id
		inner join client.products p
			on s.product_id = p.product_id
		where i.date <= '2012-03-03'
	)

	, summary as (
		select
			  stock_id
			, product_id
			, label as product_label
			, expiry_date
			, quantity as qty_remaining
			, "day"((expiry_date - cast('2012-03-03' as date))) as days_to_expiry
		from base
		where rn = 1
	)

	, final as (
		select
			  *
			, case
				when days_to_expiry <= 3 then true
				else false
			  end as less_than_3days
			, case
				when days_to_expiry <= 7 then true
				else false
			  end as less_than_1week
			, case
				when days_to_expiry <= 30 then true
				else false
			  end as less_than_1month
			, case
				when days_to_expiry > 30 then true
				else false
			  end as more_than_1month
		from summary
		where days_to_expiry >= 0
	)

	select * from final;

----------------------------------------------------------------
	
	
	
	
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
