--Task 3. Implement row-level security
--Read about row-level security (https://www.postgresql.org/docs/12/ddl-rowsecurity.html) 
--Configure that role so that the customer can only access their own data in the "rental" and "payment" tables. Write a query to make sure this user sees only their own data.


--1 variant for one customer

--vkliuchaju tablicam row level security 
alter table public.payment enable row level security;
alter table public.rental enable row level security;
alter table public.customer enable row level security;

--sozdaju novuju role iz sozdanogo spiska ranee (kto imeet zapisi v db v tablicah rental i payment)
create role client_mary_smith;

--podkliucaju usera k scheme
grant usage on schema public to client_mary_smith;

-- daju razreshenije na prosmotr k tablicam useru
grant select on public.payment to client_mary_smith;
grant select on public.rental to client_mary_smith;
grant select on public.customer to client_mary_smith;

--cozdaju politiku bezopastnosti na tablicy dlja usera iz tablic, kak by sravnivaetsja kazdaja strochka DB s sozdanoj rolju na sootvetsvije 
--i esli sootvetstvuet to togda pokazyvaet
create policy acount_customer on public.customer 
for select to client_mary_smith
using (current_user = 'client_' || lower(first_name) || '_' || lower(last_name));

create policy acount_payment on public.payment
for select to client_mary_smith
using (customer_id = (select c.customer_id 
            		 from public.customer c
            		 where lower(c.first_name) = split_part(current_user, '_', 2) and 
                		   lower(c.last_name)  = split_part(current_user, '_', 3)));

create policy acount_rental on public.rental
for select to client_mary_smith
--uslovie kotoroje dolzhno byt vypolneno dlja primenenija politiki bezopastnosti
using (customer_id = (select c.customer_id 
            		 from public.customer c
            		 where lower(c.first_name) = split_part(current_user, '_', 2) and 
                		   lower(c.last_name)  = split_part(current_user, '_', 3)));
             		         		                  		  
--Proverka
set role client_mary_smith;
set role postgres; 
select * 
from public.payment p;
select * 
from public.rental r;  

--select *
--from rental p 
--where customer_id = (select customer_id from public.customer where upper(first_name) = 'MARY' and upper(last_name) = 'SMITH')


--2 variant. Sozdaju mnogo userov i daju im pravo prosmatrivat svoju info v payment i rental tables. Vse srazu odnim mahom

do $$
declare
	rec record;
	role_name text;
	role_exists boolean;
	policy_exists boolean;
begin

--1. sozdaju grupu clients

if not exists (
	select from pg_catalog.pg_roles
	where rolname = 'clients') then 
	create role clients;	
end if;

-- 2.sozdaju polzovatelej, kotoryje imejut hotia by odin zakaz
	
-- perebiraju polzovatelej kotoryje imejut hot odin zakaz
for rec in 
	(select distinct c.customer_id, c.first_name, c.last_name
	from customer c
	join rental r on c.customer_id = r.customer_id
	join payment p on c.customer_id = p.customer_id
	where p.amount is not null and r.rental_date is not null)
loop
-- prisvaivaju role imia i familiju + vperedi nazvanije client
role_name := 'client_' || lower(rec.first_name) || '_' || lower(rec.last_name);
-- proveriaju sozdana li eta role
select exists ( 
	select 1
	from pg_roles
	where rolname = role_name
	) into role_exists;

	if not role_exists then
	execute 'create role ' || quote_ident(role_name);
	end if;

end loop;

--3. podkliuchaju k grupe clients, kotoryje imejut hotia by odin zakaz
for rec in 
	(select distinct c.customer_id, c.first_name, c.last_name
	from customer c
	join rental r on c.customer_id = r.customer_id
	join payment p on c.customer_id = p.customer_id
	where p.amount is not null and r.rental_date is not null)
loop
-- prisvaivaju role imia i familiju + vperedi nazvanije client
role_name := 'client_' || lower(rec.first_name) || '_' || lower(rec.last_name);
-- proveriaju sozdana li eta role
select exists ( 
	select 1
	from pg_roles
	where rolname = role_name
	) into role_exists;

	if role_exists then
	execute 'grant clients to ' || quote_ident(role_name);
	end if;
end loop;


--4. daju grupe clients razreshenije na schemu

grant usage on schema public to clients;

--5. daju grupe clients razreshenije podkliuchitsja k db

grant connect on database dvdrental to clients;

--6. daju grupe clients razreshenije na prosmotr k tablicam 

grant select on public.payment to clients;
grant select on public.rental to clients;
grant select on public.customer to clients;


-- 7. vkliuchaju row level security dlja tablic

alter table public.customer enable row level security;
alter table public.payment enable row level security;
alter table public.rental enable row level security;


-- 8. sozdaju politiki row level security dlja kazhdoj roli

for rec in 
			select rolname
			from pg_roles
			where rolname like 'client_%'
loop
		select exists (
			select 1
			from pg_policies
			where policyname = 'policy_paym_' || quote_ident(rec.rolname)
			and tablename = 'payment'
		) into policy_exists;
		if not policy_exists then 		
		execute 'create policy policy_paym_' || quote_ident(rec.rolname) || 
		' on public.payment for select to ' || quote_ident(rec.rolname) ||
		' using (customer_id = (select c.customer_id
		from public.customer c 
		where lower(c.first_name) = split_part(current_user, ''_'', 2)
		and lower(c.last_name) = split_part(current_user, ''_'', 3)))';
		end if;
	end loop;


for rec in 
			select rolname
			from pg_roles
			where rolname like 'client_%'
	loop
		select exists (
			select 1
			from pg_policies
			where policyname = 'policy_rent_' || quote_ident(rec.rolname)
			and tablename = 'rental'
		) into policy_exists;
		if not policy_exists then 		
		execute 'create policy policy_rent_' || quote_ident(rec.rolname) || 
		' on public.rental for select to ' || quote_ident(rec.rolname) ||
		' using (customer_id = (select c.customer_id
		from public.customer c 
		where lower(c.first_name) = split_part(current_user, ''_'', 2)
		and lower(c.last_name) = split_part(current_user, ''_'', 3)))';
		end if;
	end loop;

for rec in 
		select rolname
		from pg_roles
		where rolname like 'client_%'
	loop
	-- prisvaivaju role imia i familiju + vperedi nazvanije client 'client_firstname_lastname'
		role_name := 'client_' || lower(rec.rolname);
		select exists (
			select 1
			from pg_policies
			where policyname = 'policy_cust_' || quote_ident(rec.rolname)
			and tablename = 'customer'
		) into policy_exists;
		
		if not policy_exists then
		execute 'create policy policy_cust_' || quote_ident(rec.rolname) || 
		' on public.customer for select to ' || quote_ident(rec.rolname) ||
		' using (current_user = ''client_'' || lower(first_name) || ''_'' || lower(last_name))';
		end if;
	end loop;
end $$;


--Proverka 
set role clents;
set role client_craig_morrell;
set role client_gail_knight;
set role client_aaron_selby;
set role  postgres;
select current_role;

select * from public.payment p;
select * from public.rental;
















