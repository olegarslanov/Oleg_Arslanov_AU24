--Task 1
--Create a query to produce a sales report highlighting the top customers with the highest sales across different sales channels.
-- This report should list the top 5 customers for each channel. Additionally, calculate a key performance indicator (KPI) called
-- 'sales_percentage,' which represents the percentage of a customer's sales relative to the total sales within their respective channel.

--Please format the columns as follows:
--Display the total sales amount with two decimal places
--Display the sales percentage with five decimal places and include the percent sign (%) at the end
--Display the result for each channel in descending order of sales


--create CTEs
with new_table as (
	select ch.channel_desc, s.amount_sold, s.cust_id, c.cust_first_name, c.cust_last_name
	from sh.sales s
	inner join sh.channels ch on s.channel_id = ch.channel_id
	inner join sh.customers c on s.cust_id = c.cust_id
	),
	ranked_table as (
	select
		cust_id,
		cust_first_name,
		cust_last_name,
		channel_desc,
		sum(amount_sold) as cust_sold,
		sum(sum(amount_sold)) over (partition by channel_desc) as total_sold_channel,
		row_number() over (partition by channel_desc order by sum(amount_sold) desc) as row_num
	from new_table
	group by channel_desc, cust_id, cust_first_name, cust_last_name
	)
--query from CTE
	select 
	cust_id,
	cust_first_name || ' ' || cust_last_name as cust_full_name,
	channel_desc,
	to_char(round(cust_sold, 2), 'FM999999999.00') as customer_sold,
	-- count procents with five decimal places and % sign
	concat(round((cust_sold/total_sold_channel)*100, 5), '%') as sales_percent,
	to_char(round(total_sold_channel, 2), 'FM999999999.00') as total_sol_channel
from ranked_table
where row_num <=5
order by channel_desc, row_num;

--Task 2
--Create a query to retrieve data for a report that displays the total sales for all products in the Photo category
-- in the Asian region for the year 2000. Calculate the overall report total and name it 'YEAR_SUM'

--Display the sales amount with two decimal places
--Display the result in descending order of 'YEAR_SUM'
--For this report, consider exploring the use of the crosstab function. Additional details and guidance can be found at this link

--1 variant with view

--added tablefunc for change rows to columns in tables
create extension if not exists tablefunc;

create or replace view temp_ranked as
	select 
		prod_name,
		country_name,
		round(sum(amount_sold), 2) as total_amount
	from (
		select 
			p.prod_name,
			p.prod_category,
			c.country_name, 
			c.country_region, 
			s.amount_sold, 
			s.time_id
		from sh.sales s
		inner join sh.products p on s.prod_id = p.prod_id
		inner join sh.customers cust on s.cust_id = cust.cust_id
		inner join sh.countries c on c.country_id = cust.country_id
	) as subquery
	where extract(year from time_id) = 2000 and country_region = 'Asia' and prod_category = 'Photo'
	group by prod_name, country_name;

--create answer table sales by countries
select 
	prod_name,
	Japan,
	Singapore,
	to_char(round(sum(coalesce(japan, 0) + coalesce(singapore, 0)) over (partition by prod_name), 2),'FM999999999.00') as year_sum
from (
-- used function crosstab for create cross table
	select *
	from crosstab (
		'select prod_name, country_name, total_amount from temp_ranked order by 1, 2',
		'select distinct country_name from temp_ranked order by 1'
	) as ct(prod_name varchar(50), Japan numeric, Singapore numeric)
) as subquery;


--2 variant with temp table
with 
	new_table as (
	select 
		p.prod_id, 
		p.prod_name,
		p.prod_category,
		c.country_id, 
		c.country_name, 
		c.country_region_id, 
		c.country_region, 
		s.amount_sold, 
		s.time_id
	from sh.sales s
	inner join sh.products p on s.prod_id = p.prod_id
	inner join sh.customers cust on s.cust_id = cust.cust_id
	inner join sh.countries c on c.country_id = cust.country_id
),
	ranked_table as (
	select 
		prod_name,
		country_name,
		-- for correctly formating use func to_char for format to 2 decimal after comma
		round(sum(amount_sold), 2) as total_amount
	from new_table
	where extract(year from time_id) = 2000 and country_region = 'Asia' and prod_category = 'Photo'
	group by prod_name, country_name
	)
insert into temp_ranked_table (prod_name, country_name, total_amount)
select prod_name, country_name, total_amount
from ranked_table
where (prod_name, country_name) not in (
select prod_name, country_name
from temp_ranked_table
);	

--create temp table (for later use in crosstab, because can not use CTE inside)
create temp table if not exists temp_ranked_table (
	prod_name varchar(50),
	country_name varchar (40),
	total_amount numeric
);
	
--added tablefunc for change rows to columns in tables
create extension if not exists tablefunc;

--create answer table sales by countries
select 
	prod_name,
	Japan,
	Singapore,
	sum(coalesce(japan, 0) + coalesce(singapore, 0)) over (partition by prod_name) as year_sum
from (
-- used function crosstab for create cross table
	select *
	from crosstab (
	--in first query we need all column from result table ... using ORDER BY ensures that the distinct products are listed in alphabetical order (toest esli ukazal parametr on uze ne budet imet dublikatov)
		'select prod_name, country_name, total_amount from temp_ranked_table order by 1, 2',
	--in second query we vybiraem zagolovki columns, kotoryje budut otobrazatsja v novoj peredelannoj tablice	
		'select distinct country_name from temp_ranked_table order by 1'
	) as ct(prod_name varchar(50), Japan numeric, Singapore numeric)
) as subquery;



--Task 3
--Create a query to generate a sales report for customers ranked in the top 300 based on total sales in the years
-- 1998, 1999, and 2001. The report should be categorized based on sales channels, and separate calculations should be performed for each channel.
--Retrieve customers who ranked among the top 300 in sales for the years 1998, 1999, and 2001
--Categorize the customers based on their sales channels
--Perform separate calculations for each sales channel
--Include in the report only purchases made on the channel specified
--Format the column so that total sales are displayed with two decimal places


--query for all channels for top300 clients
select
	years,
	channel_desc,
	cust_last_name,
	cust_first_name,
	amount_sold,
	row_number
from (
	select 
		extract (year from time_id) as years,
		channel_desc,
		cust_last_name,
		cust_first_name,
		amount_sold,
--divide by channel and sort desc by amount
		row_number() over (partition by channel_desc order by amount_sold desc) as row_number
	from sh.sales s
	inner join sh.channels ch on s.channel_id = ch.channel_id
	inner join sh.customers c on s.cust_id = c.cust_id
	where extract (year from time_id) in (1998, 1999, 2001)
	) as subquery
-- how many rows needed	
where row_number <= 300;



--create func for get top300 by channel specified
create or replace function sh.channel_top300_cust (channel_desc_in varchar(20))
returns table (
    row_number bigint,
    channel_desc varchar(20),
    full_name varchar,
    amount_sold numeric   
) as $$
begin
    return query
    select
        subquery.row_number,
        ch.channel_desc,
--must change data type in same type, because func have problem with it
        cast(subquery.cust_last_name || ' ' || subquery.cust_first_name as varchar) as full_name,  -- cast full_name to varchar
        subquery.amount_sold        
    from (
        select 
            extract(year from time_id) as years,
            ch.channel_desc,
            c.cust_last_name,
            c.cust_first_name,
            s.amount_sold,
			--divide by channel and sort desc by amount
            row_number() over (partition by ch.channel_desc order by s.amount_sold desc) as row_number
        from sh.sales s
        inner join sh.channels ch on s.channel_id = ch.channel_id
        inner join sh.customers c on s.cust_id = c.cust_id
        
        where extract(year from time_id) in (1998, 1999, 2001) 
        and ch.channel_desc = channel_desc_in
    ) as subquery
    inner join sh.channels ch on ch.channel_desc = subquery.channel_desc
	-- how many rows needed	
    where subquery.row_number <= 300;
end;
$$ language plpgsql;
	
-- call func	
select * from sh.channel_top300_cust('Internet');	
select * from sh.channel_top300_cust('Direct Sales');	
	
	
--Task 4
--Create a query to generate a sales report for January 2000, February 2000, and March 2000 specifically for the Europe and Americas regions.
--Display the result by months and by product category in alphabetical order.

-- 1variant
--create view
create or replace view prod_region_date as
	select 
		extract (year from s.time_id) || '-' || extract (month from s.time_id) as year_month,
		country_region as region,
		prod_category,
		sum(amount_sold) as total_sale
	from 
		sh.sales as s
		inner join sh.customers cust on s.cust_id = cust.cust_id 
		inner join sh.countries c on c.country_id = cust.country_id
		inner join sh.products p on p.prod_id = s.prod_id
	where 
		extract (year from s.time_id) = 2000 and
		extract (month from s.time_id) in (1, 2, 3) and
		country_region in ('Europe', 'Americas')
	group by
		extract (year from s.time_id),
		extract (month from s.time_id),
		country_region,
		prod_category;
	
--use query with subquery ... renaiming columns and show only is not null	
	select *
	from (
	select 
		year_month,
		prod_category,
		total_sale as europa,
		lag(total_sale) over (partition by year_month, prod_category order by region) as americas,
		sum(total_sale) over (partition by year_month, prod_category order by region) as total
	from prod_region_date
) as subquery
where americas is not null;
	
	
	
--2 variant. here I probe do it with crosstab ... and something wrong	

--create view
create or replace view prod_region_date as
	select 
		extract (year from s.time_id) || '-' || extract (month from s.time_id) as year_month,
		country_region as region,
		prod_category,
		sum(amount_sold) as total_sale
	from 
		sh.sales as s
		inner join sh.customers cust on s.cust_id = cust.cust_id 
		inner join sh.countries c on c.country_id = cust.country_id
		inner join sh.products p on p.prod_id = s.prod_id
	where 
		extract (year from s.time_id) = 2000 and
		extract (month from s.time_id) in (1, 2, 3) and
		country_region in ('Europe', 'Americas')
	group by
		extract (year from s.time_id),
		extract (month from s.time_id),
		country_region,
		prod_category;

--something wrong here ... doubling and nulls
select *
from 
crosstab(
--in first query we need all column from result table ... using ORDER BY ensures that the distinct products are listed in alphabetical order (toest esli ukazal parametr on uze ne budet imet dublikatov)
'SELECT year_month, prod_category, region, total_sale FROM prod_region_date ORDER BY prod_category, region',
--in second query we vybiraem zagolovki columns, kotoryje budut otobrazatsja v novoj peredelannoj tablice
'SELECT DISTINCT region FROM prod_region_date ORDER BY region'
) as ct(year_month varchar, prod_category varchar, Americas numeric(10, 2), Europe numeric(10, 2));
	


	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

--Here my drafts ... not execute :)
--simple primer crosstab
CREATE TABLE sales (
	salesperson VARCHAR(50),
	product VARCHAR(50),
	sales_amount NUMERIC
	);

INSERT INTO sales (salesperson, product, sales_amount)
VALUES
	('Alice', 'Apples', 50),
	('Alice', 'Oranges', 30),
	('Bob', 'Apples', 20),
	('Bob', 'Oranges', 40),
	('Charlie', 'Apples', 70),
	('Charlie', 'Oranges', 60);

INSERT INTO sales (salesperson, product, sales_amount)
VALUES
	('Alice', 'Banana', 100);


-- Используем функцию crosstab для создания перекрестной таблицы
SELECT *
FROM crosstab(
	'SELECT salesperson, product, sales_amount FROM sales ORDER BY 2',
	'SELECT DISTINCT product FROM sales ORDER BY 1'
) AS ct(salesperson VARCHAR(50), apples NUMERIC, arbuz numeric, arbuz2 numeric, oranges numeric, banana numeric);


-- funkcija
create or replace function get_product_prices(in salesperson_in varchar, out sales_amounts numeric []
) as $$

begin
	select  array_agg(s.sales_amount) into sales_amounts
	from sales s where s.salesperson = salesperson_in;

end;
$$ language plpgsql;

select get_product_prices ('Alice');



--Retrieve the total sales amount for each product category for a specific time period
with table_sales_period as (
	select prod_category, time_id, amount_sold
	from sh.sales s
	inner join sh.products p on s.prod_id = p.prod_id
	where time_id >= '1998-01-01' and time_id <= '1998-01-31'
)
-- then I wrote query and use CTE. Here I can group by prod_category and sum all amounts
select prod_category, (select min(time_id)) || ' and ' || (select max(time_id)) as time_period, sum(amount_sold) as total_amount 
from table_sales_period
group by prod_category
order by prod_category


--Calculate the average sales quantity by region for a particular product
with 
table_prod_reg_quant as (
	select p.prod_id, prod_name, country_region, cus.cust_id, quantity_sold
	from sh.sales s
	inner join sh.products p on p.prod_id = s.prod_id
	inner join sh.customers cus on s.cust_id = cus.cust_id
	inner join sh.countries cnt on cnt.country_id = cus.country_id
)
select prod_id, prod_name, country_region, avg(quantity_sold)
from table_prod_reg_quant 
group by prod_id, prod_name, country_region
order by prod_id;


--Find the top five customers with the highest total sales amount
select cust_id, customer_name, total_sales
from (
select c.cust_id, cust_first_name || ' ' || cust_last_name as customer_name, sum(amount_sold) as total_sales
from sh.sales s
inner join sh.customers c on c.cust_id = s.cust_id
group by c.cust_id
) as new_table
order by total_sales desc
limit 5;