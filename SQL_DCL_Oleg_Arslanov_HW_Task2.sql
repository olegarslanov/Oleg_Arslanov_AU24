
--Task 2. Implement role-based authentication model for dvd_rental database
--Create a new user with the username "rentaluser" and the password "rentalpassword". Give the user the ability to connect to the database but no other permissions.

do $$
begin
	if not exists (
		select from pg_catalog.pg_roles
		where rolname = 'rentaluser') then 
	create role rentaluser with login password 'rentalpassword';	
	end if;
end
$$;

grant connect on database dvdrental to rentaluser;



--*revoke connect on database dvdrental from rentaluser;
--*drop user rentaluser;


--Grant "rentaluser" SELECT permission for the "customer" table. Сheck to make sure this permission works correctly—write a SQL query to select all customers.


grant select on table public.customer to rentaluser; 

select *
from customer


--Create a new user group called "rental" and add "rentaluser" to the group. 

do $$
begin
	if not exists (
		select from pg_catalog.pg_roles
		where rolname = 'rental') then 
	create role rental;	
	end if;
end
$$;

grant rental to rentaluser;
--revoke postgres from rentaluser;


--Grant the "rental" group INSERT and UPDATE permissions for the "rental" table. Insert a new row and update one existing row in the "rental" table under that role. 

grant insert on table public.rental to rental;
grant update on table public.rental to rental;



with 
	inventory_data as (
	select inventory_id
	from public.inventory i
	inner join public.film f on i.film_id = f.film_id
	where upper(title) = 'TERMINATOR' 
),
	customer_data as (
	select customer_id
	from public.customer c 
	where upper(first_name) || ' ' || upper(last_name) = 'OLEG ARSLANOV'
)
insert into public.rental (rental_date, inventory_id, customer_id, return_date, staff_id)
select current_date, inventory_id, customer_id, current_date, 1
from (
	select *
	from inventory_data
	cross join customer_data) as new_table
	where not exists (
		select 1
		from public.rental 
		where inventory_id = new_table.inventory_id and customer_id = new_table.customer_id
);
returning;


update public.rental 
set rental_date = current_date
where rental_id = 1000;


--Revoke the "rental" group's INSERT permission for the "rental" table. Try to insert new rows into the "rental" table make sure this action is denied.

revoke insert on table public.rental from rental;


--Create a personalized role for any customer already existing in the dvd_rental database. The name of the role name must be client_{first_name}_{last_name} (omit curly brackets).
--The customer's payment and rental history must not be empty. 



--- dont know but it is not correctly maded rolnames :( I checked it not corectly working in Task Nr.3
do $$
declare
    rec record;
    role_name text;
    role_exists boolean;
begin
    for rec in 
        (select distinct c.customer_id, c.first_name, c.last_name
         from customer c
         join rental r on c.customer_id = r.customer_id
         join payment p on c.customer_id = p.customer_id
         where p.amount is not null and r.rental_date is not null)
    loop
        role_name := 'client_' || upper(rec.first_name) || '_' || upper(rec.last_name);
        select exists ( 
             select 1
             from pg_roles
             where rolname = role_name
        ) into role_exists;
        if not role_exists then
            execute 'create role ' || quote_ident(role_name); 
        end if;
    end loop;
end $$;


-- roles created  
SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolconnlimit, rolvaliduntil
FROM pg_roles;





