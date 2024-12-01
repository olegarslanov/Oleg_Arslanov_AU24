--Create a physical database

-- create db I dont know how to made reusable ... if You run second time please comment this command
create database household_appliances_store;

create schema if NOT exists training_data;

--1 Create first table with PK without FK
create table if not exists training_data.products (
	product_id 			integer generated always as identity primary key,
	prod_name			varchar(50),
	prod_brand			varchar(50),
	prod_model			varchar(50),
	prod_price  		decimal(10, 2),
	prod_description  	text 
);

create table if not exists training_data.suppliers (
	supplier_id 	  	integer generated always as identity  primary key,
	sup_name		  	varchar(50),
	sup_contact_name  	varchar(50),
	sup_phone		  	varchar(50),
	sup_email		  	varchar(100)
);

create table if not exists training_data.employees (
	employee_id 		integer generated always as identity primary key,
	emp_name 			varchar(50),
	emp_surname 		varchar(50),
	emp_position		varchar(50),
	emp_hire_date 		date,
	emp_email			varchar(100),
	emp_phone			varchar(50)
);

create table if not exists training_data.customers (
	customer_id 	  	integer generated always as identity primary key,
	cust_name	 		varchar(50),
	cust_surname		varchar(50),
	cust_email			varchar(100),
	cust_phone			varchar(50)
);


--2 create tables with PK and/or FK (referenced on created tables)

create table if not exists training_data.inventory (
	inventory_id 	  	integer generated always as identity primary key,
	product_id		  	integer,
	inv_quantity	  	integer,
	inv_last_updated    date,
	foreign key (product_id) references training_data.products (product_id)
);

create table if not exists training_data.procurements (
	procurement_id 	  	integer generated always as identity primary key,
	supplier_id	 		integer,
	product_id			integer,
	employee_id			integer,
	proc_date			date,
	proc_quantity		integer,
	proc_total_cost		decimal(10,2),
	foreign key (supplier_id) references training_data.suppliers(supplier_id),
	foreign key (product_id) references training_data.products(product_id),	
	foreign key (employee_id) references training_data.employees(employee_id)
);

create table if not exists training_data.orders (
	order_id 		  	integer generated always as identity primary key,
	customer_id 		integer,
	employee_id 		integer,
	ord_date			date,
	foreign key (customer_id) references training_data.customers (customer_id),
	foreign key (employee_id) references training_data.employees (employee_id)
);


--3 now can created tables with more FK

create table if not exists training_data.order_details (
	order_id 			integer,
	product_id			integer,
	ord_det_quantity	integer,
	ord_det_unit_price 	decimal(10,2),
	ord_det_status		varchar(20),
	primary key 		(order_id, product_id),
	foreign key (order_id) references training_data.orders (order_id),
	foreign key (product_id) references training_data.products (product_id)
);


--Added additional constraints

--check date > 2024-07-01

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'inventory'
		and constraint_name = 'check_date'
) 
then 
alter table training_data.inventory 
add constraint check_date check (inv_last_updated > '2024-07-01');
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'orders'
		and constraint_name = 'check_date1'
) 
then 
alter table training_data.orders
add constraint check_date1 check (ord_date > '2024-07-01');
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'procurements'
		and constraint_name = 'check_date2'
) 
then 
alter table training_data.procurements
add constraint check_date2 check (proc_date > '2024-07-01');
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'employees'
		and constraint_name = 'check_date3'
) 
then 
alter table training_data.employees
add constraint check_date3 check (emp_hire_date > '2024-07-01');
end if;
end $$;


--check measured value >= 0
do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'products'
		and constraint_name = 'check_not_negative_value'
) 
then 
alter table training_data.products
add constraint check_not_negative_value check (prod_price >= 0);
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'inventory'
		and constraint_name = 'check_not_negative_value1'
) 
then 
alter table training_data.inventory
add constraint check_not_negative_value1 check (inv_quantity >= 0);
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'procurements'
		and constraint_name = 'check_not_negative_value2'
) 
then 
alter table training_data.procurements
add constraint check_not_negative_value2 check (proc_quantity >= 0 and proc_total_cost >= 0);
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'order_details'
		and constraint_name = 'check_not_negative_value3'
) 
then 
alter table training_data.order_details
add constraint check_not_negative_value3 check (ord_det_quantity >= 0 and ord_det_unit_price >= 0);
end if;
end $$;


--check specific value
do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'order_details'
		and constraint_name = 'check_specific_value'
) 
then 
alter table training_data.order_details
add constraint check_specific_value check (upper(ord_det_status) in ('PENDING', 'SHIPPED', 'DELIVERED'));
end if;
end $$;


-- check unique
do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'suppliers'
		and constraint_name = 'check_unique_email_sup'
) 
then 
alter table training_data.suppliers
add constraint check_unique_email_sup unique (sup_email);
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'employees'
		and constraint_name = 'check_unique_email_emp'
) 
then 
alter table training_data.employees
add constraint check_unique_email_emp unique (emp_email);
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'customers'
		and constraint_name = 'check_unique_email_cust'
) 
then 
alter table training_data.customers
add constraint check_unique_email_cust unique (cust_email);
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'order_details'
		and constraint_name = 'check_unique_key_ord_det'
) 
then 
alter table training_data.order_details
add constraint check_unique_key_ord_det unique (order_id, product_id);
end if;
end $$;


-- added not null constraints
do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'suppliers'
		and column_name = 'sup_email'
		and is_nullable  = 'no'
) 
then
alter table training_data.suppliers
alter column sup_email set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'employees'
		and column_name = 'emp_email'
		and is_nullable  = 'no'
) 
then
alter table training_data.employees
alter column emp_email set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'customers'
		and column_name = 'cust_email'
		and is_nullable  = 'no'
) 
then
alter table training_data.customers
alter column cust_email set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'suppliers'
		and column_name = 'sup_phone'
		and is_nullable  = 'no'
) 
then
alter table training_data.suppliers
alter column sup_phone set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'employees'
		and column_name = 'emp_phone'
		and is_nullable  = 'no'
) 
then
alter table training_data.employees
alter column emp_phone set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'customers'
		and column_name = 'cust_phone'
		and is_nullable  = 'no'
) 
then
alter table training_data.customers
alter column cust_phone set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'suppliers'
		and column_name = 'sup_name'
		and is_nullable  = 'no'
) 
then
alter table training_data.suppliers
alter column sup_name set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'employees'
		and column_name = 'emp_name'
		and is_nullable  = 'no'
) 
then
alter table training_data.employees
alter column emp_name set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'customers'
		and column_name = 'cust_name'
		and is_nullable  = 'no'
) 
then
alter table training_data.customers
alter column cust_name set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'products'
		and column_name = 'prod_name'
		and is_nullable  = 'no'
) 
then
alter table training_data.products
alter column prod_name set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'order_details'
		and column_name = 'ord_det_status'
		and is_nullable  = 'no'
) 
then
alter table training_data.order_details
alter column ord_det_status set not null;
end if;
end $$;


--added default values
do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'products'
		and column_name = 'prod_brand'
		and column_default is not null
) 
then
alter table training_data.products
alter column prod_brand set default 'no name';
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'supplier'
		and column_name = 'sup_contact_name'
		and column_default is not null
) 
then
alter table training_data.suppliers
alter column sup_contact_name set default 'no name';
end if;
end $$;


--populated tables with sample data generated

--1
insert into training_data.products (prod_name, prod_brand, prod_model, prod_price, prod_description)
select 'product name' || i, 'product brand' || i, 'product model' || i, i*10, 'product description' || i
from generate_series(1,6) as s(i)
where not exists (
	select 1 from training_data.products where upper(prod_name) = upper('product name' || i)
)
returning *;

insert into training_data.suppliers (sup_name, sup_contact_name, sup_phone, sup_email)
select 'supplier name' || i, 'supplier contact name' || i, '370-6617178' || i, 'supplier' || i || '@example.com'
from generate_series(1,6) as s(i)
where not exists (
	select 1 from training_data.suppliers where upper(sup_name) = upper('supplier name' || i)
)
returning *;


insert into training_data.employees (emp_name, emp_surname, emp_position, emp_hire_date, emp_email, emp_phone)
select 
'employee name ' || i, 
'employee surname ' || i, 
'employee position' || i, 
current_date - (random() * 90)::int, 
'employee' || i || '@example.com', 
'375-6617178' || i 
from generate_series(1,6) as s(i)
where not exists (
select 1 from training_data.employees where emp_email = 'employee' || i || '@example.com'
)
returning *;


insert into training_data.customers (cust_name, cust_surname, cust_email, cust_phone)
select 'customer name' || i, 'customer surname' || i, 'customer' || i || '@example.com', '379-6617178' || i 
from generate_series(1,6) as s(i)
where not exists (
	select 1 from training_data.customers where upper(cust_name) = upper('customer name' || i)
)
returning *;

--2
insert into training_data.inventory (product_id, inv_quantity, inv_last_updated)
select product_id, (random() * 100)::int, current_date - (random() * 90)::int
from training_data.products tdp
where not exists (
select 1 from training_data.inventory where product_id = tdp.product_id
)
returning *;

insert into training_data.procurements (supplier_id, product_id, employee_id, proc_date, proc_quantity, proc_total_cost)
select 
supplier_id, 
product_id, 
employee_id, 
current_date - (random() * 90)::int as proc_date, 
(random() * 100)::int as proc_quantity, 
(random() * 1000)::numeric(10, 2) as proc_total_cost
from 
-- used cartesian product (dekartovo proizvedenije k kazhdoj strochke pervoj tablicy prisodiniajutsja kazdaja strochka 
--iz sledujushej tablicy sozdavaja vse varianty soedinenij i tak dalee ... ne poluchilos sdelat lucshe, malo vremeni, speshu .... nu ja nichego ne narushaju nado 6+)   
training_data.suppliers sup,
training_data.products prod,
training_data.employees emp
where not exists (
select 1 from training_data.procurements 
where supplier_id = sup.supplier_id 
and product_id = prod.product_id 
and employee_id = emp.employee_id
)
returning *;

insert into training_data.orders (customer_id, employee_id, ord_date)
select 
tdc.customer_id, 
tde.employee_id, 
current_date - (random() * 90)::int as ord_date
from 
training_data.customers tdc,
training_data.employees tde
where not exists (
select 1 from training_data.orders 
where customer_id = tdc.customer_id
and employee_id = tde.employee_id
)
returning *;

--3
insert into training_data.order_details (order_id, product_id, ord_det_quantity, ord_det_unit_price, ord_det_status)
select 
tdo.order_id, 
tdp.product_id, 
(random() * 100)::int as ord_det_quantity, 
(random() * 100)::decimal(10, 2) as ord_det_unit_price, 
(array['PENDING', 'SHIPPED', 'DELIVERED'])[floor(random() * 3 + 1)] as ord_det_status
from 
training_data.products tdp,
training_data.orders tdo
where not exists (
select 1 from training_data.order_details
where product_id = tdp.product_id 
and order_id = tdo.order_id
)
returning *;
-- ok pust budet tak poka ... ja hotel 6 strochek avto sozdat, polucaetsja bolse dekartovo znachenije:/



---sozdaju view dlja vychislenija ord_total_amount , tak kak ja ne mogu zalozhit podschet pri sozdanij tablicy (potomu cto parametry nahodiatsja v tablice order_details)
create or replace view training_data.order_summary as
select 
o.order_id,
o.customer_id,
o.employee_id,
o.ord_date,
sum(od.ord_det_quantity * od.ord_det_unit_price) as ord_total_amount
from 
training_data.orders o
join 
training_data.order_details od on o.order_id = od.order_id
group by 
o.order_id, o.customer_id, o.employee_id, o.ord_date;

--select *
--from training_data.order_summary



--5. Create the following functions.
--5.1 Create a function that updates data in one of your tables. This function should take the following input arguments:
--The primary key value of the row you want to update
--The name of the column you want to update
--The new value you want to set for the specified column

--This function should be designed to modify the specified row in the table, updating the specified column with the new value.


create or replace function training_data.update_product(
p_prod_name varchar,
p_column_name varchar,
p_new_value varchar
)
returns void as $$
begin
-- proveriaju sushestvuet li prod_name
if not exists (select 1 from training_data.products where prod_name = p_prod_name) then
	raise exception 'Product name = % does not exist', p_prod_name;
end if;
-- obnovliaju znachenije po stolbcu i po stroke
if p_column_name = 'prod_name' then
	update training_data.products 
	set prod_name = p_new_value 
	where prod_name = p_prod_name;
else
	raise exception 'Invalid column name: %', p_column_name;
end if;
end;
$$ language plpgsql;


--zapuskaju funkciju
select training_data.update_product('product name1', 'prod_name', 'XXX');
--proverka cto tam nadelalo
--select * from training_data.products


--5.2 Create a function that adds a new transaction to your transaction table. 
--You can define the input arguments and output format. 
--Make sure all transaction attributes can be set with the function (via their natural keys). 
--The function does not need to return a value but should confirm the successful insertion of the new transaction.


create table if not exists training_data.transactions2 (
transaction_id        integer generated always as identity primary key,
customer_id           integer,
cust_name             varchar,
cust_surname          varchar,
employee_id           integer,
emp_name              varchar,
emp_surname           varchar,
transaction_date      date,
transaction_amount    decimal(10, 2),
transaction_type      varchar(20)
);

--Funkcija add_transaction2:
create or replace function training_data.add_transaction2(
p_cust_name varchar,
p_cust_surname varchar,
p_emp_name varchar,
p_emp_surname varchar,
p_transaction_date date,
p_transaction_amount numeric,
p_transaction_type varchar
)
returns void as $$
declare
v_customer_id integer;
v_employee_id integer;
begin

-- poluchaju customer_id
select customer_id into v_customer_id
from training_data.customers
where cust_name = p_cust_name
and cust_surname = p_cust_surname
limit 1;

-- proveriaju naiden li customer_id
if v_customer_id is null then
raise exception 'Customer not found for the given details';
end if;

-- poluchaju employee_id
select employee_id into v_employee_id
from training_data.employees
where emp_name = p_emp_name
and emp_surname = p_emp_surname
limit 1;

-- proveriaju naiden li employee_id
if v_employee_id is null then
raise exception 'Employee not found for the given details';
end if;

-- dobavliaju tranzakciju
insert into training_data.transactions2 (
customer_id, cust_name, cust_surname, employee_id, emp_name, emp_surname, transaction_date, transaction_amount, transaction_type
) values (
v_customer_id, p_cust_name, p_cust_surname, v_employee_id, p_emp_name, p_emp_surname, p_transaction_date, p_transaction_amount, p_transaction_type
);
end;
$$ language plpgsql;


select training_data.add_transaction2(
'customer name1', 'customer surname1', 'employee name 1', 'employee surname 1', current_date, 100.00, 'Sale'
);

--select * from training_data.transactions2;



--6. Create a view that presents analytics for the most recently added quarter in your database. Ensure that the result
--excludes irrelevant fields such as surrogate keys and duplicate entries.

create or replace view training_data.quarter_analytic as
--ispolzuem CTE vremennuju tablicu dlja legkosti chitanija koda ... tut poluchaem nachalo chetverti
with recent_quarter as (
select 
--funkcija date_trunc ispolzuetsja dlja usechenija daty, vremeni do ukazanogo intervala.. zdes nahodim nachalo poslednego kvartala
date_trunc('quarter', max(ord_date)) as last_quarter_start
from training_data.orders
),
--poluchaem vremennuju otfiltrovannuju tablicu zakazov 
filtered_orders as (
select 
o.order_id,
o.ord_date,
c.cust_name,
c.cust_surname,
e.emp_name,
e.emp_surname,
sum(od.ord_det_quantity * od.ord_det_unit_price) as total_order_amount
from 
training_data.orders o
join training_data.customers c on o.customer_id = c.customer_id
join training_data.employees e on o.employee_id = e.employee_id
join training_data.order_details od on o.order_id = od.order_id
join recent_quarter rq on o.ord_date >= rq.last_quarter_start
--filtruem zakazy cto byli sdelany vo vremia poslednego kvartala
where o.ord_date < rq.last_quarter_start + interval '3 months' and 
o.ord_date >= rq.last_quarter_start
group by o.order_id, o.ord_date, c.cust_name, c.cust_surname, e.emp_name, e.emp_surname
)
select 
ord_date,
cust_name,
cust_surname,
emp_name,
emp_surname,
total_order_amount
from 
filtered_orders;

--ispolzuem view ... vse prekrasno
--select * from training_data.quarter_analytic;




--7. Create a read-only role for the manager. This role should have permission to perform SELECT queries on the database
--tables, and also be able to log in. Please ensure that you adhere to best practices for database security when defining this role

--Odna iz best security practik eto row level security i politika bezopastnosti security policy


--Create for one user row security level

--create the role with login privileges
create role manager with login password 'manager_password';

-- create employee1 and assign to manager role
create role employee_1;
grant manager to employee_1;

-- create employee2 and assign to manager role
create role employee_2;
grant manager to employee_2;

-- grant usage on schema
grant usage on schema training_data to manager;

--grant select permissions on all tables
grant select on all tables in schema training_data to manager;
 
--ensure future tables are included by setting default privileges
alter default privileges in schema training_data grant select on tables to manager;

-- enable Rls for 1 table
alter table training_data.employees enable row level security;

--create rls for 1 table
create policy acount_employee_1 on training_data.employees
for select to employee_1
using (split_part(emp_name, ' ', 1) = split_part(current_user, '_', 1) and 
       split_part(emp_surname, ' ', 3)  = split_part(current_user, '_', 2));

   

--enable row-level security for all tables in the schema .. ok
do $$
declare
r record;
begin
for r in (
select table_name
from information_schema.tables
where table_schema = 'training_data'
and table_type = 'base table'
) loop
execute format('alter table training_data.%i enable row level security;', r.table_name);
end loop;
end $$;

---here I tried to create rls for all tables ... ja dumaja nado ispolzovat if else ... poka ne polucaetsja 
--create row-level security policies for all tables

--do $$
--declare
--r record;
--begin
--for r in (select tablename from pg_tables where schemaname = 'training_data') loop
--execute format(
--'create policy account_%I on training_data.%I for select to manager
--using (split_part(emp_name, '' '', 1) = split_part(current_user, ''_'', 1) 
--and split_part(emp_surname, '' '', 3) = split_part(current_user, ''_'', 2));',
--r.tablename, r.tablename
--);
--end loop;
--end $$;

--set role 
--set role manager;
--set role employee_1;
--set role postgres;
--select current_user;

--select * from training_data.employees;

--- nu mozhet esli by bylo bolshe vremeni mozhet i poluchilos by :)























