--Task 3. Implement row-level security
--Read about row-level security (https://www.postgresql.org/docs/12/ddl-rowsecurity.html) 
--Configure that role so that the customer can only access their own data in the "rental" and "payment" tables. Write a query to make sure this user sees only their own data.



--1 variant for one customer

create role client_mary_smith;


alter table public.customer enable row level security;

alter table public.payment enable row level security;

alter table public.rental enable row level security;


grant usage on schema public to client_mary_smith;

grant select on public.payment to client_mary_smith;

grant select on public.rental to client_mary_smith;


create policy acount_customer on public.customer
for select to client_mary_smith
using (lower(first_name) = split_part(current_user, '_', 2) and 
       lower(last_name)  = split_part(current_user, '_', 3));

grant select (customer_id, first_name, last_name)
on public.customer to client_mary_smith;


create policy acount_payment on public.payment
for select to client_mary_smith
using (customer_id = (select c.customer_id 
            		 from public.customer c
            		 where lower(c.first_name) = split_part(current_user, '_', 2) and 
                		   lower(c.last_name)  = split_part(current_user, '_', 3)));


create policy acount_rental on public.rental
for select to client_mary_smith
using (customer_id = (select c.customer_id 
            		 from public.customer c
            		 where lower(c.first_name) = split_part(current_user, '_', 2) and 
                		   lower(c.last_name)  = split_part(current_user, '_', 3)));


set role client_mary_smith; 

select * 
from public.payment p;

select * 
from public.rental r;  


--2 variant I want added to all customers (who have one or more order) ... I try firstly do it for customers table   


alter table public.customer enable row level security;

--created role clients, because i want add all roles at once
do $$
begin
    if not exists (
        select from pg_catalog.pg_roles
        where rolname = 'clients'
    ) then
        create role clients;
    end if;
end $$;


--i made anonymous block for adding in clents all created roles in task 2 (customers who have rows in rental and payment) to role clients
do $$
declare
    role_name record;
begin
    for role_name in
        select rolname
        from pg_roles
        where rolname like 'client_%'
    loop
        if role_name.rolname != 'clients' then
            execute 'grant clients to ' || quote_ident(role_name.rolname);
        end if;
    end loop;
end $$;


do $$
declare
    role_name record;
begin
    for role_name in
        select rolname
        from pg_roles
        where rolname like 'client_%'
    loop
        if role_name.rolname != 'clients' then
			execute 'grant usage on schema public to ' || quote_ident(role_name.rolname);
		end if;
    end loop;
end $$;


do $$
declare
    role_name record;
begin
    for role_name in
        select rolname
        from pg_roles
        where rolname like 'client_%'
    loop
        if role_name.rolname != 'clients' then
            execute 'grant select on table public.customer to ' || quote_ident(role_name.rolname);
        end if;
    end loop;
end $$;


-- create policy for customer table
create policy account_customers on public.customer to clients 
using ('client_' || upper(first_name) || '_' || upper(last_name) = current_user);

--- and something wrong  :/