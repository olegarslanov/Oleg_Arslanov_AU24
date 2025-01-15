
--Task 2. Implement role-based authentication model for dvd_rental database

--1. Create a new user with the username "rentaluser" and the password "rentalpassword". Give the user the ability to connect to the database but no other permissions.

--sozdaju rentaluser role s login. sozdaju anonymous block, dlja togo ctob ne sozdavat esli role rentaluser sozdana(ispolzuju if else)
do $$
begin
	if not exists (
		select from pg_catalog.pg_roles
		where rolname = 'rentaluser') then 
	create role rentaluser with login password 'rentalpassword';	
	end if;
end
$$;

-- daju privilegij na podkliuchenije k dvdrental db rentaluseru 
grant connect on database dvdrental to rentaluser;
--revoke connect on database dvdrental from rentaluser;

--2. Grant "rentaluser" SELECT permission for the "customer" table. Сheck to make sure this permission works correctly—write a SQL query to select all customers.
-- dobavliaju razreshenije na viu schemu
grant usage on schema public to rentaluser;
--revoke usage on schema public from rentaluser;
-- daju privilegiju na prosmotr dannyh v tablice customer rentaluseru (nado snacla dat razresenija na Db, potom na schemu ...)
grant select on table public.customer to rentaluser; 
--revoke select on table public.customer from rentaluser;

---Proverka vse li rabotaet
--sozdaju rol rentalusera
set role rentaluser;

-- query to select all customers
select * from public.customer;


--3. Create a new user group called "rental" and add "rentaluser" to the group. 

--sozdaju grupu rental v a.b. ctob ne vydavalo oshibku, esli role sozdana
do $$
begin
	if not exists (
		select from pg_catalog.pg_roles
		where rolname = 'rental') then 
	create role rental;	
	end if;
end
$$;

--dobavliaju rentaluser v sozdannuju rental gruppu
grant rental to rentaluser;


--4. Grant the "rental" group INSERT and UPDATE permissions for the "rental" table. 
--Insert a new row and update one existing row in the "rental" table under that role. 

--daju privilegiju roli rental insert i update v tablice rental
grant insert, update on table public.rental to rental;

-- podkliuchajus v role ... dlja etogo nuzno srazu podkliucit rental k DB, potom k scheme, 
-- dalee podkliucit tablichki cto sviazany v moem insert zaprose 
grant connect on database dvdrental to rental;
grant usage on schema public to rental;
grant select on table public.rental to rental;
grant select on table public.inventory to rental;
grant select on table public.film to rental;
grant select on table public.customer to rental;

--podkliuchajus k role
set role rental;
-- vvozhu novuju stroku v rental tablicu s rental role
with 
inventory_data as (
	select inventory_id
	from public.inventory i
	inner join public.film f on i.film_id = f.film_id
	where upper(title) = 'MATRICA' 
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

-- update stroku
update public.rental 
set rental_date = current_date
where rental_id = 1000;


--5. Revoke the "rental" group's INSERT permission for the "rental" table. Try to insert new rows into the "rental" table make sure this action is denied.

-- ubiraju permission na insert rental roli
set role postgres;
revoke insert on table public.rental from rental;
--REVOKE insert ON COLUMN public.rental.last_update FROM rental;

--Proverka
--podkliuchajus k role
set role rental;
-- vvozhu novuju stroku v rental tablicu s rental role
with 
inventory_data as (
	select inventory_id
	from public.inventory i
	inner join public.film f on i.film_id = f.film_id
	where upper(title) = 'MATRICA' 
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
-- update stroku
update public.rental 
set rental_date = current_date
where rental_id = 1000;



--6. Create a personalized role for any customer already existing in the dvd_rental database. The name of the role name
-- must be client_{first_name}_{last_name} (omit curly brackets). The customer's payment and rental history must not be empty. 


--sozdaju avtomatom rolej kuchu
set role postgres;

do $$
declare
    rec record;
    role_name text;
    role_exists boolean;
begin
	--perebiraju vseh userov kotoryje imejut hotia by odin zakaz ... togda po oceredi kazdogo v rec i dalee v cikl 'loop'
    for rec in 
        (select distinct c.customer_id, c.first_name, c.last_name
         from customer c
         join rental r on c.customer_id = r.customer_id
         join payment p on c.customer_id = p.customer_id
         where p.amount is not null and r.rental_date is not null)
    loop
		-- snachala prisvoil role_name znachenija first_name i last_name poluchaemoje is for cikla
        role_name := 'client_' || lower(rec.first_name) || '_' || lower(rec.last_name);
		--teper proveriaju sozdana li eta role, esli net sozdaju
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







---- zdes nizhe moj zamorochki, ne zapuskat!!!
---------------------------
set role postgres;

--posmotret vse roli to cto est
select rolname from pg_roles;
--udalit vse role like 'client%'
do $$
declare
	role_name text;
begin
	for role_name in
		select rolname
		from pg_roles
		where rolname like 'client_%'
	loop
		execute format('drop role %I', role_name);
	end loop;
end $$;
-- udalit odnu role
drop role "user_oleg";
--esli ne udaetsja udalit ... uiraem vse objekty prinadlezhashije role
DROP OWNED BY "client_aaron_selby";
--udalenije vseh objektov prinadlezhashih role (esli problema pri udalenii)
do $$
declare
	role_name text;
begin
	for role_name in
		select rolname
		from pg_roles
		where rolname like 'client_%'
	loop
		execute format ('drop owned by %I', role_name);
	end loop;
end $$;


--proveriaem cto za privilegii imejutsja
SELECT grantee, privilege_type
FROM information_schema.column_privileges
WHERE table_name = 'rental' AND column_name = 'last_update';
--udalit mnogo privilegii roliam like 'client%'
do $$
declare
	role_name text;
begin
	for role_name in
		select rolname
		from pg_roles
		where rolname like 'client_%'
	loop
		execute format('revoke all privileges on all tables in schema public from %I', role_name);
	end loop;
end $$;
--Chtob udalit privilegij na role
revoke connect on database test_data from "user_olegas";
revoke all on schema public from "user_olegas";
revoke all privileges on all tables in schema public from "client_CRAIG_MORRELL";
revoke all privileges on table public.test from "user_oleg";
revoke all on sequence test_id_seq from "user_olegas";


--proverit cto za politika bezopastnosti vkliuchena
SELECT 
    schemaname, 
    tablename, 
    policyname, 
    permissive, 
    cmd, 
    roles
FROM 
    pg_policies;
--udalieam politki bezopastnosti
DROP POLICY policy_cust_client_aaron_selby ON public.payment;


--prosmotr kakim tablicam vkliuchen RLS
SELECT 
    relname, 
    relrowsecurity 
FROM 
    pg_class
WHERE 
    relrowsecurity = true;
-- otkliuchaju RLS 
ALTER TABLE rental DISABLE ROW LEVEL SECURITY;

--------------------------------------








