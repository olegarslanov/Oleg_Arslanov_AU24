--Part 1: Write SQL queries to retrieve the following data

--1) All animation movies released between 2017 and 2019 with rate(rental_rate) more than 1, alphabetical

select *
from film f
where release_year >= 2017 
	and release_year <= 2019 
	and f.rental_rate > 1
order by title asc;

select *
from film
where release_year between 2017 and 2019 
	and rental_rate > 1
order by title;

--2) The revenue earned by each rental store since March 2017 (columns: address and address2 – as one column, revenue)
 
select 
	coalesce(address, '') || ' ' || coalesce(address2, '') as full_address,
	sum(amount) as revenue
from (
	select address,	address2, amount
	from payment p
	join customer c on p.customer_id = c.customer_id 
	join (select store_id, address, address2 FROM store s JOIN address a ON s.address_id = a.address_id) store_adr on store_adr.store_id = c.store_id
	where payment_date >= '2017-03-01') new_table
group by address, address2;
	
-- with CTE
with store_address as (
	select store_id, address, address2
	from store s
		join address a on s.address_id = a.address_id
)
select coalesce(address, '') || ' ' || coalesce(address2, '') as full_address, sum(amount) as revenue
from (
	select address, address2, amount
	from payment p
	join customer c on p.customer_id = c.customer_id 
	join store_address on store_address.store_id = c.store_id
	where payment_date >= '2017-03-01') new_table
group by address, address2;


--3) Top-5 actors by number of movies (released since 2015) they took part in (columns: first_name, last_name, number_of_movies, 
--sorted by number_of_movies in descending order)

select first_name, last_name, count(f.film_id) as number_of_movies
from actor a 
join film_actor fa on a.actor_id = fa.actor_id
join film f on fa.film_id = f.film_id
where release_year >= 2015
group by first_name, last_name
order by number_of_movies desc
limit 5;

--CTE
with joined_table AS (
	select first_name, last_name, f.film_id, f. release_year
	from actor a 
	join film_actor fa on a.actor_id = fa.actor_id
	join film f on fa.film_id = f.film_id
	where release_year >= 2015
)
select first_name, last_name, count(film_id) as number_of_movies
from joined_table
group by first_name, last_name
order by number_of_movies desc
limit 5;


--4) Number of Drama, Travel, Documentary per year (columns: release_year, number_of_drama_movies, number_of_travel_movies, 
--number_of_documentary_movies), sorted by release year in descending order. Dealing with NULL values is encouraged

select release_year, 
	count(*) filter (where c.category_id =7) as number_of_drama_movies, 
	count(*) filter (where c.category_id =16) as number_of_travel_movies,
	count(*) filter (where c.category_id =6) as number_of_documentary_movies
from film f
join film_category fc on fc.film_id = f.film_id 
join category c on fc.category_id = c.category_id
group by c.category_id, release_year
having count(*) filter (where c.category_id=7) > 0 or 
	count(*) filter (where c.category_id=16) > 0 or 
	count(*) filter (where c.category_id=6) > 0
order by release_year desc;




--5) For each client, display a list of horrors that he had ever rented (in one column, separated by commas), and the amount of money that he paid for it

with table_distinct as (
	select distinct first_name || ' ' || last_name as client, f.title, amount, category_id, payment_id
	from customer c
	join rental r on c.customer_id = r.customer_id 
	join payment p on c.customer_id = p.customer_id
	join inventory i on r.inventory_id = i.inventory_id
	join film f on f.film_id = i.film_id
	join film_category fc on fc.film_id = f.film_id
	where category_id = 11
),
client_film_amount as (
	select client, title, sum(amount) as total_sum
	from table_distinct
	group by client, title
)
select client, string_agg(title, ', ') as films_name, total_sum
from client_film_amount
group by client, total_sum
order by client;


--Part 2: Solve the following problems using SQL

--1. Which three employees generated the most revenue in 2017? They should be awarded a bonus for their outstanding performance. 
--Assumptions: 
--staff could work in several stores in a year, please indicate which store the staff worked in (the last one);
--if staff processed the payment then he works in the same store; 
--take into account only payment_date

--- there is two CTE, then I joined them
with staff_revenue as (
	select p.staff_id, SUM(amount) as total_revenue
	from staff s 
	join payment p on p.staff_id = s.staff_id 
	join store stor on stor.store_id = s.store_id
	where extract(year from payment_date) = 2017
	group by p.staff_id
),
last_staff_store as (
	select s.staff_id, stor.store_id as last_store, max(payment_date) as last_store_staff
	from staff s 
	join payment p on p.staff_id = s.staff_id 
	join store stor on stor.store_id = s.store_id
	where extract(year from payment_date) = 2017
	group by s.staff_id, stor.store_id)
select sr.staff_id, total_revenue, last_store
from staff_revenue sr
join last_staff_store lss on sr.staff_id = lss.staff_id
order by total_revenue desc
limit 3;

-- there I can aggregate by one CTE, because I can group by the same attributes
with staff_revenue as (
    select s.staff_id, st.store_id, sum(p.amount) as total_revenue, max(p.payment_date) as last_payment_date
    from payment p
    join staff s on p.staff_id = s.staff_id
    join store st on s.store_id = st.store_id
    where extract (year from p.payment_date) = 2017
    group by s.staff_id, st.store_id
)
select staff_id, store_id, total_revenue
from staff_revenue
order by total_revenue desc
limit 3;
   

--2. Which 5 movies were rented more than others (number of rentals), and what's the expected age of the audience for these movies? 
--To determine expected age please use 'Motion Picture Association film rating system'

with film_count as (
select f.title, count(rental_id) as count_max
from film f
join inventory i on f.film_id = i.film_id 
join rental r on i.inventory_id = r.inventory_id
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
join film_rating fr on fc.title = fr.title


select title_r as film_title, rating as Motion_Picture_Association_film_rating_system
from (
	select f.title as title_r, count(rental_id) as count_max
	from film f
	join inventory i on f.film_id = i.film_id 
	join rental r on i.inventory_id = r.inventory_id
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

--V1 version. We can and need join actor, film and film_actor tables, because we need operate with attributes from this tables. From them we need actors name, surname and films releases, where actor played. 
--Count gap between extract(year from current_date) and last release_year, where actor played. And then we only need order by asc
select
    a.first_name || ' ' || a.last_name as actor_name,
    extract(year from current_date) - max(f.release_year) as years_last_release
from actor a
join film_actor fa on a.actor_id = fa.actor_id
join film f on fa.film_id = f.film_id
group by a.first_name, a.last_name
order by years_last_release desc;
	
--V2 version. I use CTE for clear view. I made two table: release_year and lag_release_table. Those table are connected twice by actor_id and release_year.
-- Function LAG automaticly always taking previous value of release year of film, where actor filmed (so we need that connection release year). 
--So then we need to connect tables, select MAX period grouping by name, surname. And miracle thats it :)   
with release_year as (
	select 
		a.actor_id,
		a.first_name,
		a.last_name,
		release_year
	from actor a
	join film_actor fa on a.actor_id = fa.actor_id
	join film f on fa.film_id = f.film_id),
lag_release_year as (
	select 
		a.actor_id,
		a.first_name,
		a.last_name,
		release_year,
		lag(release_year) over(partition by a.actor_id order by release_year) as previous_val
	from actor a
	join film_actor fa on a.actor_id = fa.actor_id
	join film f on fa.film_id = f.film_id)
select
    ry.first_name || ' ' || ry.last_name as actor_name,
    max(ry.release_year - previous_val) as max_difference
from release_year ry
join lag_release_year lry on ry.actor_id = lry.actor_id
	and ry.release_year = lry.release_year
group by ry.first_name, ry.last_name
order by max_difference desc;





