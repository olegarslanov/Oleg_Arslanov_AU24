--Task 1. Window Functions
--Create a query to generate a report that identifies for each channel and throughout the entire period, the regions with the highest 
--quantity of products sold (quantity_sold). 

--The resulting report should include the following columns:
--*CHANNEL_DESC
--*COUNTRY_REGION
--*SALES: This column will display the number of products sold (quantity_sold) with two decimal places.
--*SALES %: This column will show the percentage of maximum sales in the region (as displayed in the SALES column) compared to the total sales for that channel. The sales percentage should be displayed with two decimal places and include the percent sign (%) at the end.
--Display the result in descending order of SALES


with 
sales_channel as (
	select 
		channel_desc,
		sum(quantity_sold) as sale_channel
	from
		sh.sales s
		inner join sh.channels ch using (channel_id)
	group by channel_desc
),
sales_channel_region as (
	select 
		channel_desc,
		country_region,
		--sum(sum(quantity_sold)) over (order by 
		sum(quantity_sold) as total_sales_ch_region,
		max(sum(quantity_sold)) over (partition by channel_desc) as max_sales_region
	from 
		sh.sales s
		inner join sh.channels ch using (channel_id)
		inner join sh.customers cust using (cust_id)
		inner join sh.countries c using (country_id)
	group by channel_desc, country_region	
)	
select	
	scr.channel_desc,
	scr.country_region,
	to_char(round(max_sales_region, 2), 'FM9,999,999,999,999,999.00') as sales,
	to_char(round(100 * max_sales_region/sale_channel, 2), 'FM9,999,999,999,999,999.00') || '%' as "sales %"
from 
	sales_channel_region scr
	inner join sales_channel sc using (channel_desc)
where max_sales_region = total_sales_ch_region
order by max_sales_region desc;	
	
	
	
--Task 2. Window Functions
--Identify the subcategories of products with consistently higher sales from 1998 to 2001 compared to the previous year. 

--*Determine the sales for each subcategory from 1998 to 2001.
--*Calculate the sales for the previous year for each subcategory.
--*Identify subcategories where the sales from 1998 to 2001 are consistently higher than the previous year.
--*Generate a dataset with a single column containing the identified prod_subcategory values.


select 
	prod_subcategory_desc
from (
	select 
		prod_subcategory_desc,
		extract (year from time_id) as year,
		sum(amount_sold),
		rank () over(partition by prod_subcategory_desc order by sum(amount_sold)) as rank_num
	from 
		sh.sales s
		inner join sh.products p using (prod_id)
	where extract (year from time_id) in (1998, 1999, 2000, 2001)	
	group by prod_subcategory_desc, extract (year from time_id)
) as subquery
--group by create group for using having later
group by prod_subcategory_desc
-- having using for filter prod_subcategory
having 
	sum(case when year = 1998 and rank_num = 1 then 1 else 0 end) > 0 and
	sum(case when year = 1999 and rank_num = 2 then 1 else 0 end) > 0 and
	sum(case when year = 2000 and rank_num = 3 then 1 else 0 end) > 0 and
	sum(case when year = 2001 and rank_num = 4 then 1 else 0 end) > 0;
	


--Task 3. Window Frames
--Create a query to generate a sales report for the years 1999 and 2000, focusing on quarters and product categories. In the report
--you have to  analyze the sales of products from the categories 'Electronics,' 'Hardware,' and 'Software/Other,' across the
--distribution channels 'Partners' and 'Internet'.

--The resulting report should include the following columns:
--*CALENDAR_YEAR: The calendar year
--*CALENDAR_QUARTER_DESC: The quarter of the year
--*PROD_CATEGORY: The product category
--*SALES$: The sum of sales (amount_sold) for the product category and quarter with two decimal places
--*DIFF_PERCENT: Indicates the percentage by which sales increased or decreased compared to the first quarter of the year. For the 
--first quarter, the column value is 'N/A.' The percentage should be displayed with two decimal places and include the percent sign (%)
--at the end.
--*CUM_SUM$: The cumulative sum of sales by quarters with two decimal places
--*The final result should be sorted in ascending order based on two criteria: first by 'calendar_year,' then by 'calendar_quarter_desc';
-- and finally by 'sales' descending


	
with 
new_table as (	
	select 
		extract (year from time_id) as calendar_year,
		extract (quarter from time_id) as calendar_quarter_desc,
		prod_category,
		sum(amount_sold) as sales$,
		sum(sum(amount_sold)) over (partition by extract (year from time_id) order by extract (quarter from time_id) 
		groups between unbounded preceding and current row) as cum_sum$
	from 
		sh.sales s
		inner join sh.products p using (prod_id)
		inner join sh.channels ch using (channel_id)
	where 
		extract (year from time_id) in (1999, 2000) 
		and prod_category in ('Electronics', 'Hardware', 'Software/Other') 
		and channel_desc in ('Partners', 'Internet') 
	group by 
		extract (year from time_id),
		extract (quarter from time_id),
		prod_category
),
table_for_final_calculation as (
	select 
		calendar_year,
		calendar_quarter_desc,
		prod_category,
		sales$,
		first_value(sales$) over (partition by prod_category, calendar_year order by calendar_quarter_desc) as first_quarter_sales$,
		cum_sum$
	from
		new_table
)	
select 
	to_char(calendar_year, '9999') as calendar_year,
	calendar_year || '-0' || calendar_quarter_desc as calendar_quarter_desc,
	prod_category,
	to_char(round(sales$, 2), '9,999,999,999.99') as sales$,
	case 
		when calendar_quarter_desc = 1 then 'N/A'
		else to_char(100* (sales$ - first_quarter_sales$)/first_quarter_sales$, '9999999999.99') || '%'
	end as diff_percent,
	cum_sum$
from table_for_final_calculation
order by calendar_year asc, calendar_quarter_desc asc, sales$ desc;

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	