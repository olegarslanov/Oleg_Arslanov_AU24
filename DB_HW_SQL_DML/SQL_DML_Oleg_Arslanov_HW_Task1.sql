--Task 1

--Choose your top-3 favorite movies and add them to the 'film' table (films with the title Film1, Film2, etc - will not be taken into account and grade will be reduced)
--Fill in rental rates with 4.99, 9.99 and 19.99 and rental durations with 1, 2 and 3 weeks respectively.


-- I need add new rows in the table. I added all new films at once (used union all for adding new rows in the new_film table). I choose only NOT NULL attributes, 
--because there must be not null. We need to be sure that new rows is not in db, so I use syntax where not exists. And it is checks if in db same values, if not it add new rows.  
insert into public.film (title, language_id, rental_rate, rental_duration)
select *
from (
	select upper('cloud atlas'), 1, 4.99, 1
	union all 
	select upper('matrica'), 1, 9.99, 2
	union all
	select upper('terminator'), 1, 19.99, 3
) as new_film (title, language_id, rental_rate, rental_duration)
where not exists (select 1 from film where title = new_film.title)
returning *;



--Add the actors who play leading roles in your favorite movies to the 'actor' and 'film_actor' tables (6 or more actors in total).
--  Actors with the name Actor1, Actor2, etc - will not be taken into account and grade will be reduced.

--  there I added all rows in one temporary table (union all command) for reduce code redudancy 
insert into public.actor (first_name, last_name)
select * 
from (
	select upper('Arnold'), upper('Schwarzenegger')
	union all 
	select upper('Keanu'), upper('Reevs')
	union all
	select upper('Carrie-Anne'), upper('Moss')
	union all
	select upper('Tom'), upper('Hanks')
	union all
	select upper('Halle'), upper('Berry')
	union all
	select upper('Jim'), upper('Sturgess')
	union all
	select upper('Hugh'), upper('Grant')
) as new_actor(first_name, last_name)
where not exists (select 1 from actor where actor.first_name = new_actor.first_name and actor.last_name = new_actor.last_name)
returning *;

-- I got existing film_id and actor_id values and inserted in film_actor table for connection between tables actor and film.


insert into public.film_actor (actor_id, film_id)
select actor_id, film_id
from (
	select actor_id, film_id
	from public.actor a
		cross join public.film f 
	where (first_name || ' ' || last_name = 'KEANU REEVS' 
		or first_name || ' ' || last_name = 'CARRIE-ANNE MOSS') 
		and title = 'MATRICA'
) as new_f_a (actor_id, film_id)
where not exists (
	select 1 
	from film_actor fa 
	where fa.actor_id = new_f_a.actor_id 
		and fa.film_id = new_f_a.film_id
)
returning actor_id, film_id;		
		

insert into public.film_actor (actor_id, film_id)
select actor_id, film_id
from (
	select actor_id, film_id
	from public.actor a
		cross join public.film f 
	where upper(first_name || ' ' || last_name) = 'ARNOLD SCHWARZENEGGER' 
		and upper(title) = 'TERMINATOR'
) as new_f_a (actor_id, film_id)
where not exists (
	select 1 
	from film_actor fa 
	where fa.actor_id = new_f_a.actor_id 
		and fa.film_id = new_f_a.film_id
)
returning actor_id, film_id;		


insert into public.film_actor (actor_id, film_id)
select actor_id, film_id
from (
	select actor_id, film_id
	from public.actor a
		cross join public.film f 
	where (first_name || ' ' || last_name = 'JIM STURGESS' 
		or first_name || ' ' || last_name = 'HALLE BERRY'
		or first_name || ' ' || last_name = 'TOM HANKS'
		or first_name || ' ' || last_name = 'HUGH GRANT')
		and title = 'CLOUD ATLAS'
) as new_f_a (actor_id, film_id)
where not exists (
	select 1 
	from film_actor fa 
	where fa.actor_id = new_f_a.actor_id 
		and fa.film_id = new_f_a.film_id
)
returning actor_id, film_id;		
		


-- ! I know that I will need add values in film_category and category tables, but this action is not in HW !


--Add your favorite movies to any store's inventory.

-- there I added values for atributes film_id and adress_id for conection with other tables


insert into public.inventory (film_id, store_id)
select *
from (
	select film_id, store_id
	from public.film
		cross join public.store
	where upper(title) = 'MATRICA'
		and store_id = 1
) new_i (film_id, store_id)
where not exists (
	select 1
	from public.inventory i
	where new_i.film_id = i.film_id 
		and new_i.store_id = i.store_id
)
returning film_id, store_id;

insert into public.inventory (film_id, store_id)
select *
from (
	select film_id, store_id
	from public.film
		cross join public.store
	where upper(title) = 'TERMINATOR'
		and store_id = 1
) new_i (film_id, store_id)
where not exists (
	select 1
	from public.inventory i
	where new_i.film_id = i.film_id 
		and new_i.store_id = i.store_id
)
returning film_id, store_id;

insert into public.inventory (film_id, store_id)
select *
from (
	select film_id, store_id
	from public.film
		cross join public.store
	where upper(title) = 'CLOUD ATLAS'
		and store_id = 1
) new_i (film_id, store_id)
where not exists (
	select 1
	from public.inventory i
	where new_i.film_id = i.film_id 
		and new_i.store_id = i.store_id
)
returning film_id, store_id;



--Alter any existing customer in the database with at least 43 rental and 43 payment records. Change their personal data
-- to yours (first name, last name, address, etc.). You can use any existing address from the "address" table. Please do not
-- perform any updates on the "address" table, as this can impact multiple records with the same address.

-- I found customer with > 43 rentals/payments. I used inner join to made table there I can count it. And I remember id 598.


-- This is example of the CTE You asked in interview :|
with select_customer as (
select customer_id 
from (
	select p.customer_id, 
		count(r.rental_id) as count_rental,
		count(payment_id) as count_payment
	from public.rental r
	inner join public.payment p on r.rental_id = p.rental_id
	group by p.customer_id
	having count(r.rental_id) > 42 and count(payment_id) > 42 
	order by customer_id desc 
	limit 1 
) as subquery
)
select p.customer_id, 
	count(r.rental_id) as count_rental,
	count(payment_id) as count_payment
from public.rental r
inner join public.payment p on r.rental_id = p.rental_id
group by p.customer_id
having count(r.rental_id) > 42 and count(payment_id) > 42 
order by random() 
limit 1; 
--Updates the customer with customer_id: 598

update public.customer
set first_name = upper('Oleg'),
	last_name = upper('Arslanov'),
	email = upper ('olegarslanov')|| '' ||'@yahoo.com',
	address_id = 600 



--Remove any records related to you (as a customer) from all tables except 'Customer' and 'Inventory'

--I remove records from rental and payment tables. And dont remove from address table, because think it is bad practice we must have info about customer 

with select_customer2 as (
	select customer_id
	from customer c
	where upper(first_name) = 'OLEG' and upper(last_name) = 'ARSLANOV'
)
delete 
from public.payment p
where customer_id = (select customer_id from select_customer2)
	

with select_customer2 as (
	select customer_id
	from customer c
	where upper(first_name) = 'OLEG' and upper(last_name) = 'ARSLANOV'
)
delete 
from public.rental 
where customer_id = (select customer_id from select_customer2)


delete from public.payment 
where customer_id = 598;

delete from public.rental 
where customer_id = 598;



--Rent you favorite movies from the store they are in and pay for them (add corresponding records to the database to represent this activity)
--(Note: to insert the payment_date into the table payment, you can create a new partition (see the scripts to install the training database ) or add records for the
--first half of 2017)


--create partition of payment table
create table payment_2024 partition of public.payment

for values from ('2024-01-01') to ('2024-12-31')
where not exists(
	select 1
	from public.payment p
	where p.payment_2024 = payment_2024
);


-- I think I leave like dont working ... but did that

with select_inventory_id as (
	select inventory_id
	from inventory i 
		inner join film f on f.film_id = i.film_id
	where upper(f.title) = 'CLOUD ATLAS'
),
select_customer_id as (
	select c.customer_id, 1 as staff_id, current_timestamp as rental_date, current_timestamp as return_date
	from	
		customer c
	where 
		upper(first_name) = 'OLEG'
		and upper(last_name) = 'ARSLANOV'
)
insert into public.rental (customer_id, staff_id, rental_date, return_date, inventory_id)
select *
from 
	select_customer_id sci
	cross join select_inventory_id sii 
where not exists(
	select 1
	from public.rental r
	where inventory_id = sii.inventory_id
		and customer_id = sci.customer_id
		and staff_id = sci.staff_id
		and rental_date::date = current_date
)
returning *;	

with select_inventory_id as (
	select inventory_id
	from inventory i 
		inner join film f on f.film_id = i.film_id
	where upper(f.title) = 'MATRICA'
),
select_customer_id as (
	select c.customer_id, 1 as staff_id, current_timestamp as rental_date, current_timestamp as return_date
	from	
		customer c
	where 
		upper(first_name) = 'OLEG'
		and upper(last_name) = 'ARSLANOV'
)
insert into public.rental (customer_id, staff_id, rental_date, return_date, inventory_id)
select *
from 
	select_customer_id sci
	cross join select_inventory_id sii 
where not exists(
	select 1
	from public.rental r
	where inventory_id = sii.inventory_id
		and customer_id = sci.customer_id
		and staff_id = sci.staff_id
		and rental_date::date = current_date
)
returning *;	


with select_inventory_id as (
	select inventory_id
	from inventory i 
		inner join film f on f.film_id = i.film_id
	where upper(f.title) = 'TERMINATOR'
),
select_customer_id as (
	select c.customer_id, 1 as staff_id, current_timestamp as rental_date, current_timestamp as return_date
	from	
		customer c
	where 
		upper(first_name) = 'OLEG'
		and upper(last_name) = 'ARSLANOV'
)
insert into public.rental (customer_id, staff_id, rental_date, return_date, inventory_id)
select *
from 
	select_customer_id sci
	cross join select_inventory_id sii 
where not exists(
	select 1
	from public.rental r
	where inventory_id = sii.inventory_id
		and customer_id = sci.customer_id
		and staff_id = sci.staff_id
		and rental_date::date = current_date
)
returning *;	


insert into payment_2024 (customer_id, staff_id, rental_id, amount, payment_date)
select *
from (
	select r.customer_id, staff_id, rental_id, rental_rate as amount, current_timestamp as payment_date
	from
		rental r
		inner join customer c on r.customer_id = c.customer_id
		inner join inventory i on r.inventory_id = i.inventory_id
		inner join film f on f.film_id = i.film_id
	where upper(title) = 'MATRICA'
		and upper(first_name) = 'OLEG'
		and upper(last_name) = 'ARSLANOV'
) as new_table3
where not exists(
	select 1
	from public.payment p
	where 
		customer_id = new_table3.customer_id
		and staff_id = new_table3.staff_id
		and rental_id = new_table3.rental_id
		and amount = new_table3.amount
		and payment_date::date = current_date
)
returning *;


insert into payment_2024 (customer_id, staff_id, rental_id, amount, payment_date)
select *
from (
	select r.customer_id, staff_id, rental_id, rental_rate as amount, current_timestamp as payment_date
	from
		rental r
		inner join customer c on r.customer_id = c.customer_id
		inner join inventory i on r.inventory_id = i.inventory_id
		inner join film f on f.film_id = i.film_id
	where upper(title) = 'TERMINATOR'
		and upper(first_name) = 'OLEG'
		and upper(last_name) = 'ARSLANOV'
) as new_table3
where not exists(
	select 1
	from public.payment p
	where 
		customer_id = new_table3.customer_id
		and staff_id = new_table3.staff_id
		and rental_id = new_table3.rental_id
		and amount = new_table3.amount
		and payment_date::date = current_date
)
returning *;

insert into payment_2024 (customer_id, staff_id, rental_id, amount, payment_date)
select *
from (
	select r.customer_id, staff_id, rental_id, rental_rate as amount, current_timestamp as payment_date
	from
		rental r
		inner join customer c on r.customer_id = c.customer_id
		inner join inventory i on r.inventory_id = i.inventory_id
		inner join film f on f.film_id = i.film_id
	where upper(title) = 'CLOUD ATLAS'
		and upper(first_name) = 'OLEG'
		and upper(last_name) = 'ARSLANOV'
) as new_table3
where not exists(
	select 1
	from public.payment p
	where 
		customer_id = new_table3.customer_id
		and staff_id = new_table3.staff_id
		and rental_id = new_table3.rental_id
		and amount = new_table3.amount
		and payment_date::date = current_date
)
returning *;





