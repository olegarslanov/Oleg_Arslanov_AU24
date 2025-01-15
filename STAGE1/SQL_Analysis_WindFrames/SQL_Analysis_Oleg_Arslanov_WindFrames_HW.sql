--Task 1
--Create a query for analyzing the annual sales data for the years 1999 to 2001, focusing on different sales channels and 
--regions: 'Americas,' 'Asia,' and 'Europe.' 

--The resulting report should contain the following columns:
--* AMOUNT_SOLD: This column should show the total sales amount for each sales channel
--* % BY CHANNELS: In this column, we should display the percentage of total sales for each channel (e.g. 100% - total sales
-- for Americas in 1999, 63.64% - percentage of sales for the channel “Direct Sales”)
--* % PREVIOUS PERIOD: This column should display the same percentage values as in the '% BY CHANNELS' column but for the previous year
--* % DIFF: This column should show the difference between the '% BY CHANNELS' and '% PREVIOUS PERIOD' columns, indicating the
-- change in sales percentage from the previous year.
--* The final result should be sorted in ascending order based on three criteria: first by 'country_region,' then by 'calendar_year,' and finally by 'channel_desc'



-- create view 
create or replace view new_table as
select 
	row_number () over (partition by year order by country_region, channel_desc) as row_num,
	country_region,
	channel_desc,
	year,
	amount_sold,
	sum(amount_sold) over (partition by country_region, year) as total_year_region
	--sum(amount_sold) over (partition by country_region order by country_region range between unbounded preceding and current row) as total_region
from (
	select 
		c.country_region,
		ch.channel_desc,
		extract (year from s.time_id) as year,
		--wholw table sum amounts group by year, country_region, channel_desc
		round(sum(s.amount_sold), 0) as amount_sold
	from 
		sh.sales s
		inner join sh.channels ch on s.channel_id = ch.channel_id
		inner join sh.customers cust on s.cust_id = cust.cust_id
		inner join sh.countries c on c.country_id = cust.country_id
	where 
		country_region in ('Americas', 'Asia', 'Europe') and 
		extract (year from s.time_id) in (1998, 1999, 2000, 2001) and channel_desc != 'Tele Sales'
	group by c.country_region, ch.channel_desc, extract (year from s.time_id)
	order by year
) as subquery
order by year, country_region, channel_desc;


--query for get answer
select
	country_region,
	channel_desc,
	year,
	amount_sold,
	"% by channels",
	--precent for each country_region, channel_desc and year from sales to total sale of that group
	concat(round(100 * prev_amount_sold/prev_total_year_region, 2), '%') as "% previous period",
	--difference between percents
	concat(round((round(100 * amount_sold / total_year_region, 2) - round(100 * prev_amount_sold / prev_total_year_region, 2)), 2), '%') as "% diff"
from (
	select 
		row_num,
		country_region,
		channel_desc,
		year,
		amount_sold,
		total_year_region,
		--precent for each country_region , channel_desc and year from sales to total sale of that group
		concat(round(100 * amount_sold/total_year_region, 2), '%') as "% by channels",
		--put values in anothers cells for future countings
		lag(amount_sold, 9) over (order by year) as prev_amount_sold,
		lag(total_year_region, 9) over (order by year) as prev_total_year_region
	from new_table
) as subquery
where year in (1999, 2000, 2001);
	


--Task 2
--You need to create a query that meets the following requirements:

--1.Generate a sales report for the 49th, 50th, and 51st weeks of 1999.

--2.Include a column named CUM_SUM to display the amounts accumulated during each week.

--3.Include a column named CENTERED_3_DAY_AVG to show the average sales for the previous, current, and following days using
-- a centered moving average.
--*For Monday, calculate the average sales based on the weekend sales (Saturday and Sunday) as well as Monday and Tuesday.
--*For Friday, calculate the average sales on Thursday, Friday, and the weekend.


-- query for task
select
	*,
	case
		when week_day = 'monday' then round(avg(cum_sum) over (order by time_id rows between 2 preceding and 1 following), 2)
		--when week_day = 'friday' then round(avg(cum_sum) over (partition by cum_sum order by time_id rows between 1 preceding and 2 following), 2)
		when week_day = 'friday' then round(avg(cum_sum) over (order by time_id rows between 1 preceding and 2 following), 2)
		else round(avg(cum_sum) over (order by time_id rows between 1 preceding and 1 following), 2)
	end as centered_3_day_avg
from (
	select 
		--row_number () over (partition by extract(week from time_id) order by time_id) as row_num,
		extract(week from time_id) as week_number,
		time_id, 
		--added for get week day and 'FMday' for geting day of week without spaces
		to_char (time_id, 'FMday') as week_day,
		sum(amount_sold) as cum_sum,
		sum(sum(amount_sold)) over (partition by extract(week from time_id) order by time_id range between unbounded preceding and current row) as cum_sales
	from sh.sales s 
	where extract (year from time_id) = 1999 and extract(week from time_id) in (49, 50, 51)
	group by time_id
);


--Task 3
--Please provide 3 instances of utilizing window functions that include a frame clause, using RANGE, ROWS, and GROUPS modes. 
--Additionally, explain the reason for choosing a specific frame type for each example. 
--This can be presented as a single query or as three distinct queries.


create table if not exists example (
	country varchar, date date, sales integer
);

insert into example (country, date, sales)
values
	('usa', '2024-12-09', 100),
	('usa', '2024-12-09', 100),
 	('usa', '2024-12-10', 100),
 	('usa', '2024-12-11', 100),
 	('usa', '2024-12-13', 100),
 	('uk', '2024-12-15', 100),
 	('uk', '2024-12-16', 100),
	('uk', '2024-12-16', 100);

-- using rows
--reason for choosing rows: the rows frame is useful when you want to include a specific number of rows before and after the current row.
--in this example, it sums the sales for each country including the current row, the row before, and the row after. this is helpful when
--you want to consider a fixed number of rows around the current row.
select 
	country,
	date,
	sales,
	sum(sales) over (partition by country order by date rows between 1 preceding and 1 following) as rows_sum_sales
from example
order by date;


--using range
--Reason for Choosing RANGE: The RANGE frame is useful when you want to include all rows within a specific range of values. In this case,
--it sums the sales for each country within a 1 day range before and after the current date. This is helpful when you want to account 
--for all sales within a certain time window, regardless of the number of rows.

select 
	country,
	date,
	sales,
	sum(sales) over (partition by country order by date range between interval '1' day preceding and interval '1' day following) as range_sum_sales
from example
order by date;


--using groups
--reason for choosing groups: the groups frame is useful when you want to include groups of rows that have the same values in the
--ordering column. in this example, it sums the sales for each country including the current group of rows, the group before, and
--the group after. this is helpful when you want to consider groups of rows with the same date value

select 
	country,
	date,
	sales,
	sum(sales) over (partition by country order by date groups between 1 preceding and 1 following) as groups_sum_sales
from example
order by date;




























