--Part 1: Write SQL queries to retrieve the following data

--1) All animation movies released between 2017 and 2019 with rate(rental_rate) more than 1, alphabetical

--2024-11-03 I  added =forgoten filter by 'animation'
select f.film_id, title
from public.film f
	inner join film_category fc on fc.film_id = f.film_id
	inner join category c on c.category_id = fc.category_id
where release_year >= 2017 
	and release_year <= 2019 
	and f.rental_rate > 1
	and upper(c.name) = 'ANIMATION'
order by title asc;

select f.film_id, title
from public.film f
	inner join film_category fc on fc.film_id = f.film_id
	inner join category c on c.category_id = fc.category_id
where release_year between 2017 and 2019 
	and rental_rate > 1
	and upper(c.name) = 'ANIMATION'
order by title asc;

--2) The revenue earned by each rental store since March 2017 (columns: address and address2 – as one column, revenue)
 
--2024-11-03 I simplify query (remove unnecessary subquery)
select 
	coalesce(address, '') || ' ' || coalesce(address2, '') as full_address,
	sum(amount) as revenue
from (
	select address,	address2, amount
	from public.payment p
	inner join public.customer c on p.customer_id = c.customer_id 
	inner join public.store s on s.store_id = c.store_id
	inner join public.address a on s.address_id = a.address_id
	where payment_date >= '2017-03-01') new_table
group by full_address;
	
-- with CTE
with address_amount as (
	select address, address2, amount
	from public.payment p
	inner join public.customer c on p.customer_id = c.customer_id 
	inner join public.store s on s.store_id = c.store_id
	inner join public.address a on s.address_id = a.address_id
	where payment_date >= '2017-03-01'
)
select coalesce(address, '') || ' ' || coalesce(address2, '') as full_address, sum(amount) as revenue
from address_amount
group by full_address;


--3) Top-5 actors by number of movies (released since 2015) they took part in (columns: first_name, last_name, number_of_movies, 
--sorted by number_of_movies in descending order)

select first_name, last_name, count(f.film_id) as number_of_movies
from public.actor a 
inner join public.film_actor fa on a.actor_id = fa.actor_id
inner join public.film f on fa.film_id = f.film_id
where release_year >= 2015
group by first_name, last_name
order by number_of_movies desc
limit 5;

--CTE
with joined_table AS (
	select first_name, last_name, f.film_id, f. release_year
	from public.actor a 
	inner join public.film_actor fa on a.actor_id = fa.actor_id
	inner join public.film f on fa.film_id = f.film_id
	where release_year >= 2015
)
select first_name, last_name, count(film_id) as number_of_movies
from joined_table
group by first_name, last_name
order by number_of_movies desc
limit 5;


--4) Number of Drama, Travel, Documentary per year (columns: release_year, number_of_drama_movies, number_of_travel_movies, 
--number_of_documentary_movies), sorted by release year in descending order. Dealing with NULL values is encouraged

--2024-11-03 CTE dealing with NUll values :)

with new_table as (
	select release_year, name, fc.category_id
	from public.film f
	inner join public.film_category fc on fc.film_id = f.film_id 
	inner join public.category c on fc.category_id = c.category_id)
select 
	release_year,
	sum(coalesce(case when upper(name) = 'DRAMA' then 1 end, 0)) as number_of_drama_movies,
	sum(coalesce(case when upper(name) = 'TRAVEL' then 1 end, 0)) as number_of_travel_movies,
	sum(coalesce(case when upper(name) = 'DOCUMENTARY' then 1 end, 0)) as number_of_documentary_movies
from new_table
group by release_year
order by release_year desc;


select 
	release_year, 
	count(*) filter (where upper(name) = 'DRAMA') as number_of_drama_movies, 
	count(*) filter (where upper(name) = 'TRAVEL') as number_of_travel_movies,
	count(*) filter (where upper(name) = 'DOCUMENTARY') as number_of_documentary_movies
from public.film f
inner join public.film_category fc on fc.film_id = f.film_id 
inner join public.category c on fc.category_id = c.category_id
group by release_year
order by release_year desc;


--5) For each client, display a list of horrors that he had ever rented (in one column, separated by commas), and the amount of money that he paid for it

with new_table as (
	select c.customer_id, first_name || ' ' || last_name as client, f.title, amount, ctg.category_id
	from public.customer c
	inner join public.rental r on c.customer_id = r.customer_id 
	inner join public.payment p on p.rental_id = r.rental_id
	inner join public.inventory i on r.inventory_id = i.inventory_id
	inner join public.film f on f.film_id = i.film_id
	inner join public.film_category fc on fc.film_id = f.film_id
	inner join public.category ctg on ctg.category_id = fc.category_id
	where upper(name) = 'HORROR'
)
select customer_id, client, string_agg(title, ', ') as films_name, sum(amount) as total_sum
from new_table
group by customer_id, client
order by customer_id;



--Part 2: Solve the following problems using SQL

--1. Which three employees generated the most revenue in 2017? They should be awarded a bonus for their outstanding performance. 
--Assumptions: 
--staff could work in several stores in a year, please indicate which store the staff worked in (the last one);
--if staff processed the payment then he works in the same store; 
--take into account only payment_date

--- there is two CTE, then I joined them
with staff_revenue as (
	select p.staff_id, first_name || ' ' ||last_name as staff_name, SUM(amount) as total_revenue
	from public.staff s 
	inner join public.payment p on p.staff_id = s.staff_id 
	inner join public.store stor on stor.store_id = s.store_id
	where extract(year from payment_date) = 2017
	group by p.staff_id, staff_name
),
last_staff_store as (
	select s.staff_id, stor.store_id as last_store, max(payment_date) as last_store_staff
	from public.staff s 
	inner join public.payment p on p.staff_id = s.staff_id 
	inner join public.store stor on stor.store_id = s.store_id
	where extract(year from payment_date) = 2017
	group by s.staff_id, stor.store_id)
select sr.staff_id, staff_name, total_revenue, last_store
from staff_revenue sr
inner join last_staff_store lss on sr.staff_id = lss.staff_id
order by total_revenue desc
limit 3;

-- there I can aggregate by one CTE, because I can group by the same attributes
with staff_revenue as (
    select s.staff_id, st.store_id, sum(p.amount) as total_revenue, max(p.payment_date) as last_payment_date, first_name || ' ' ||last_name as staff_name
    from public.payment p
    inner join public.staff s on p.staff_id = s.staff_id
    inner join public.store st on s.store_id = st.store_id
    where extract (year from p.payment_date) = 2017
    group by s.staff_id, st.store_id, staff_name
)
select staff_id, staff_name, total_revenue, store_id
from staff_revenue
order by total_revenue desc
limit 3;
   

--2. Which 5 movies were rented more than others (number of rentals), and what's the expected age of the audience for these movies? 
--To determine expected age please use 'Motion Picture Association film rating system'

with film_count as (
select f.title, count(rental_id) as count_max
from public.film f
inner join public.inventory i on f.film_id = i.film_id 
inner join public.rental r on i.inventory_id = r.inventory_id
group by f.title
order by count_max desc
limit 5
),
film_rating as (
select title, rating
from film
)
select fc.title as film_title, rating as Motion_Picture_Association_film_rating_system
from film_count fc
inner join film_rating fr on fc.title = fr.title


select title_r as film_title, rating as Motion_Picture_Association_film_rating_system
from (
	select f.title as title_r, count(rental_id) as count_max
	from public.film f
	inner join public.inventory i on f.film_id = i.film_id 
	inner join public.rental r on i.inventory_id = r.inventory_id
	group by f.title
	order by count_max desc
	limit 5) as film_count
join (
	select title, rating
	from film) as film_rating on film_rating.title = film_count.title_r

	
--Part 3. Which actors/actresses didn't act for a longer period of time than the others? 

--The task can be interpreted in various ways, and here are a few options:
--V1: gap between the latest release_year and current year per each actor;
--V2: gaps between sequential films per each actor;
--It would be plus if you could provide a solution for each interpretation

--2024-11-03 Added actors didnt act for a longer period of time	
--V1 version. We can and need join actor, film and film_actor tables, because we need operate with attributes from this tables. 
--Count gap between extract(year from current_date) and last release_year, where actor played.
with actors_gap2024 as (
	select
		a.actor_id,
	    a.first_name || ' ' || a.last_name as actor_name,
	    extract(year from current_date) - max(f.release_year) as years_last_release
	from public.actor a
	inner join public.film_actor fa on a.actor_id = fa.actor_id
	inner join public.film f on fa.film_id = f.film_id
	group by a. actor_id, a.first_name, a.last_name
	order by years_last_release desc
)
select actor_id, actor_name, years_last_release
from actors_gap2024
where years_last_release = (select max(years_last_release) from actors_gap2024)


-- 2024-11-03 Reniew and change for accept requirements and code made simple
--V2 version. I use CTE for clear view. Function LAG automaticly always taking previous value of release year of film,
-- where actor filmed (so we need that connection release year). 


with new_table as (
	select distinct 
		a.actor_id,
		first_name,
		last_name,
		(release_year - (
		select 
			max(f2.release_year)
		from public.film f2
		where 
			f2.release_year < f.release_year
			and	f2.film_id in (select fa.film_id from public.film_actor fa where fa.actor_id = a.actor_id)
		)) as gap
	from public.actor a
	inner join public.film_actor fa on a.actor_id = fa.actor_id
	inner join public.film f on fa.film_id = f.film_id
)
select
	actor_id,
	first_name || ' ' || last_name as actor_name,
	gap
from new_table
where gap = (select max(gap) from new_table)






	
	





