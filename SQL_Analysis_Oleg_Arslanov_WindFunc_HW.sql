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


--added tablefunc extenssion (for change rows to columns in tables)
create extension if not exists tablefunc;

--create view
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
	--
	to_char(round(sum(coalesce(japan, 0) + coalesce(singapore, 0)) over (partition by prod_name), 2),'FM999999999.00') as year_sum
from (
-- used function crosstab for create cross table
	select *
	from crosstab (
		'select prod_name, country_name, total_amount from temp_ranked order by 1, 2',
		'select distinct country_name from temp_ranked order by 1'
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

--create CTE   
with ranking_table as(
select 
    cus.cust_id,
    cus.cust_last_name,
    cus.cust_first_name,
    c.channel_desc,
    extract (year from time_id) as years,  --extract years from dates
    sum(s.amount_sold) as amount_sold,  --sum by grouping by customer, channel, year
    dense_rank() over(partition by c.channel_desc, extract (year from time_id) order by sum(s.amount_sold) desc) as rnk  -- ranking without gaps
from 
    sh.sales s 
    inner join sh.customers cus on cus.cust_id = s.cust_id 
    inner join sh.channels c on c.channel_id = s.channel_id
where 
    extract (year from time_id) in (1998, 1999, 2001) --customer be in top300 for one time in 1998 or/and 1999 or/and 2001
group by 
    cus.cust_id,
    c.channel_desc,
    extract (year from time_id)
)
--query from ranking_table    
select 
    cust_id,
    cust_last_name,
    cust_first_name,
    channel_desc,
    to_char(sum(amount_sold), '999999999.99') as amount_sold --sum by grouping customer, channel and format to text with nice two decimals after point
from 
    ranking_table
where 
    rnk <= 300
group by 
    cust_id,
    cust_last_name,
    cust_first_name,
    channel_desc
having 
    count(years) = 3  --customers must be in all 1998 and 1999 and 2001 years 
order by 
    amount_sold desc;


-- it is no needed in this Task, but I hold it for future
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
		prod_category,
		country_region as region,
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
		lag(total_sale) over (partition by year_month, prod_category order by region) as americas,
		total_sale as europa,
		sum(total_sale) over (partition by year_month, prod_category order by region) as total
	from prod_region_date
) as subquery
where americas is not null;
	
	
	
--2 variant. here I did it with crosstab :) 

with 
	prod_region_date as (
		select 
			extract (year from s.time_id) || '-' || extract (month from s.time_id) as year_month,
			prod_category,
			country_region as region,
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
			prod_category
),
	crosstab_null as (
		select *
		from 
			crosstab(
			--in first query we need all column from result table ... using ORDER BY ensures that the distinct products are listed in alphabetical order (toest esli ukazal parametr on uze ne budet imet dublikatov)
			'SELECT year_month, prod_category, region, total_sale FROM prod_region_date ORDER BY prod_category, region, year_month',
			--in second query we choose columns for new table (vybiraem zagolovki columns, kotoryje budut otobrazatsja v novoj peredelannoj tablice)
			'SELECT DISTINCT region FROM prod_region_date ORDER BY region'
			) as ct(year_month varchar, prod_category varchar, Americas numeric(10, 2), Europe numeric(10, 2))
)
select 
	year_month,
	prod_category,
	sum(coalesce(Americas, 0)) as Americas,
	sum(coalesce(Europe, 0)) as Europe
from 
	crosstab_null
group by year_month, prod_category
order by year_month, prod_category;









	
--------------------------------------------------------------------
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
	('Charlie', 'Oranges', 60),
	('Alice', 'Banana', 100);

select *
from sales;

-- Используем функцию crosstab для создания перекрестной таблицы
SELECT *
FROM crosstab(
	--berem tablicu s kotoroj budem peredelyvat ...v order by ukazyvaju obiazatelno kakoj poriadok
	'SELECT salesperson, product, sales_amount FROM sales order by salesperson, product',
	--berem atribut s tablicy 'sales', kotoryj budem perekladyvat vverh, chtob svesti vse nu skazem apples
	'SELECT DISTINCT product FROM sales ORDER BY product'
	--zdes cetko ukazyvaem poriadok vseh nashih novyh stolbcov
) AS ct(salesperson VARCHAR(50), apples NUMERIC, arbuz numeric, arbuz2 numeric, banana numeric, oranges numeric);

	
	
-------------------------------------------
--one more example
CREATE TABLE sales5 (
    salesperson VARCHAR(50),
    product VARCHAR(50),
    quantity INT,
    total_sales NUMERIC
);

INSERT INTO sales5 (salesperson, product, quantity, total_sales) VALUES
('Alice', 'Apples', 10, 100.0),
('Alice', 'Bananas', 15, 150.0),
('Bob', 'Apples', 20, 200.0),
('Bob', 'Bananas', 25, 250.0),
('Charlie', 'Apples', 30, 300.0),
('Charlie', 'Bananas', 35, 350.0);


WITH quantity_data AS (
    SELECT * 
    FROM crosstab(
        $$SELECT salesperson, product, quantity
          FROM sales5
          ORDER BY salesperson, product$$,
        $$SELECT DISTINCT product FROM sales5 ORDER BY product$$
    ) AS ct (
        salesperson VARCHAR(50), apples_quantity INT, bananas_quantity INT
    )
),
sales_data AS (
    SELECT * 
    FROM crosstab(
        $$SELECT salesperson, product, total_sales
          FROM sales5
          ORDER BY salesperson, product$$,
        $$SELECT DISTINCT product FROM sales5 ORDER BY product$$
    ) AS ct (
        salesperson VARCHAR(50), apples_total_sales NUMERIC, bananas_total_sales NUMERIC
    )
)
SELECT 
    q.salesperson,
    q.apples_quantity,
    s.apples_total_sales,
    q.bananas_quantity,
    s.bananas_total_sales
FROM quantity_data q
JOIN sales_data s USING (salesperson);


-------------------------------------------------
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