
--Task 3. Write SQL queries to perform the following tasks:
--Retrieve the total sales amount for each product category for a specific time period

-- firstly I made CTE and got all categories + specific time period january 1998 + amount. 
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























