--Create a physical database

--1 . Create a physical database with a separate database and schema and give it an appropriate domain-related name.
--Use the relational model you've created while studying DB Basics module. Task 2 (designing a logical data model
-- on the chosen topic). Make sure you have made any changes to  your model after your mentor's comments.
 
--2. Your database must be in 3NF
--3. Use appropriate data types for each column and apply DEFAULT values, and GENERATED ALWAYS AS columns as required.
--4. Create relationships between tables using primary and foreign keys.
--5. Apply five check constraints across the tables to restrict certain values, including
--*date to be inserted, which must be greater than January 1, 2000
--*inserted measured value that cannot be negative
--*inserted value that can only be a specific value (as an example of gender)
--*unique
--*not NULL

--6. Populate the tables with the sample data generated, ensuring each table has at least two rows (for a total of 20+
--rows in all the tables).
--7. Add a not null 'record_ts' field to each table using ALTER TABLE statements, set the default value to current_date,
--and check to make sure the value has been set for the existing rows.


--Note:
--*Your physical model should be in 3nf, all constraints, data types correspond your logical model
--*Your code must be reusable and rerunnable and executes without errors
--*Your code should not produces duplicates
--*Avoid hardcoding
--*Use appropriate data types
--*Add comments (as example why you chose particular constraint, datatytpe, etc.)
--*Please attached a graphical image with your fixed logical model


-- create db I dont know how to made reusable ... when You run second time please coment this command
create database mountaineering_club;

create schema if NOT exists training_data;

--1 Create first table with PK without FK
create table if not exists training_data.sponsor
(
	sponsor_id 			int generated always as identity primary key,
	name				varchar(50),
	surname 			varchar(50),
	sponsor_type		varchar(50) 
);


create table if not exists training_data.address
(
	address_id 		  	int generated always as identity  primary key,
	country			  	varchar(50),
	city			  	varchar(50),
	street			  	varchar(50),
	house_number	  	varchar(10),
	flat_number		    integer
);

create table if not exists training_data.equipment
(
	equipment_id 		int generated always as identity primary key,
	name 				varchar(100),
	equipment_type		varchar(50)
);

create table if not exists training_data.club
(
	club_id 		  	int generated always as identity primary key,
	name			  	varchar(50),
	location			varchar(100),
	telephone_number  	varchar(30),
	email   		    varchar(30),
	description         text
);

create table if not exists training_data.climb
(
	climb_id 		  	int generated always as identity primary key,
	name        		varchar(50)
);

create table if not exists training_data.location
(
	location_id 	  	int generated always as identity primary key,
	country		 		varchar(50),
	region 				varchar(50)
);


--2 create tables with PK and/or FK (referenced on created tables)

create table if not exists training_data.climber
(
	climber_id 		  	int generated always as identity primary key,
	name			  	varchar(50),
	surname			  	varchar(50),
	experience_level  	varchar(50),
	address_id		    integer,
	foreign key (address_id) references training_data.address (address_id)
);

create table if not exists training_data.climb_schedule
(
	climb_id 		  	int,
	begin_date	 		date,
	end_date			date,
	foreign key (climb_id) references training_data.climb (climb_id)
);

create table if not exists training_data.weather
(
	weather_id 		  	int generated always as identity primary key,
	climb_id 			integer,
	temperature 		decimal(4, 1),
	wind_speed			decimal(4, 1),
	precipation			boolean,
	foreign key (climb_id) references training_data.climb (climb_id)
);

create table if not exists training_data.mountain
(
	mountain_id 		int generated always as identity primary key,
	name 		 		varchar(50),
	height              integer,
	location_id         integer,
	foreign key (location_id) references training_data.location (location_id)
);


--3 now can created tables with more FK

create table if not exists training_data.donation
(
	donation_id 		int generated always as identity primary key,
	club_id				integer,
	sponsor_id 			integer,
	climber_id 			integer,
	contribution		decimal(10, 2),
	date_sponsored  	date,
	foreign key (sponsor_id) references training_data.sponsor (sponsor_id),
	foreign key (club_id) references training_data.club (club_id),
	foreign key (climber_id) references training_data.climber (climber_id)
);

create table if not exists training_data.climber_climb
(
	row_id				int generated always as identity,
	climber_id 		  	integer,
	climb_id			integer,
	club_id				integer,
	climb_type		  	varchar(20),
	primary key  		(climber_id, climb_id, club_id),
	foreign key (climber_id) references training_data.climber (climber_id),
	foreign key (climb_id) references training_data.climb (climb_id),
	foreign key (club_id) references training_data.club (club_id)
);

create table if not exists training_data.climber_equipment
(
	row_id				int generated always as identity,
	climber_id 		  	integer,
	equipment_id 		integer,
	primary key 		(climber_id, equipment_id),
	foreign key (climber_id) references training_data.climber (climber_id),
	foreign key (equipment_id) references training_data.equipment (equipment_id)
);


create table if not exists training_data.route
(
	route_id 		  	int generated always as identity primary key,
	name		 		varchar(50),
	difficulty_level    varchar(20),
	mountain_id         int,
	foreign key (mountain_id) references training_data.mountain (mountain_id)
);


--4 last one

create table if not exists training_data.climb_route
(
	row_id				int generated always as identity,
	route_id 		  	integer,
	climb_id 		    integer,
	club_id				integer,
	primary key 		(route_id, climb_id, club_id),
	foreign key (route_id) references training_data.route (route_id),
	foreign key (climb_id) references training_data.climb (climb_id),
	foreign key (club_id) references training_data.club (club_id)
);



--Added additional HW constraints. Used anonymous blocks, because in scripts hard to realise check for
-- operation is added or not. In a.b. I can use If constructions

--check date > 2000-01-01

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'donation'
		and constraint_name = 'check_date'
) 
then 
alter table training_data.donation 
add constraint check_date check (date_sponsored > '2000-01-01');
end if;
end $$;


do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'climb_schedule'
		and constraint_name = 'check_date1'
) 
then 
alter table training_data.climb_schedule
add constraint check_date1 check (begin_date > '2000-01-01' and end_date > '2000-01-01');
end if;
end $$;


--check measured value > 0
do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'weather'
		and constraint_name = 'check_positive_value'
) 
then 
alter table training_data.weather 
add constraint check_positive_value check ( wind_speed > 0);
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'donation'
		and constraint_name = 'check_positive_value1'
) 
then 
alter table training_data.donation 
add constraint check_positive_value1 check ( contribution > 0);
end if;
end $$;


--check specific value
do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'sponsor'
		and constraint_name = 'check_specific_value'
) 
then 
alter table training_data.sponsor 
add constraint check_specific_value check (upper(sponsor_type) in ('INDIVIDUAL', 'CORPORATE', 'GOVERNMENT'));
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'equipment'
		and constraint_name = 'check_specific_value1'
) 
then 
alter table training_data.equipment
add constraint check_specific_value1 check (upper(equipment_type) in ('CLIMBING HARNESS', 'CRAMPONS', 'ICE AXE', 'CARABINER', 'ROPE', 'BELAY DEVICE', 'CLIMBING HELMET', 'AVALANCHE BEACON', 'TENT', 'SLEEPING BAG', 'STAKES AND SNOW ANCHORS', 'PITONS', 'GAITERS'));
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'route'
		and constraint_name = 'check_specific_value2'
) 
then 
alter table training_data.route
add constraint check_specific_value2 check (upper(difficulty_level) in ('EASY', 'MEDIUM', 'EXPERT'));
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'climber'
		and constraint_name = 'check_specific_value3'
) 
then 
alter table training_data.climber
add constraint check_specific_value3 check (upper(experience_level) in ('BEGINNER', 'INTERMEDIATE', 'ADVANCED'));
end if;
end $$;



-- check unique

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'club'
		and constraint_name = 'check_unique'
) 
then 
alter table training_data.club
add constraint check_unique unique (email);
end if;
end $$;


do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'climb_route'
		and constraint_name = 'check_unique1'
) 
then 
alter table training_data.climb_route
add constraint check_unique1 unique (climb_id, route_id, club_id);
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'climber_climb'
		and constraint_name = 'check_unique2'
) 
then 
alter table training_data.climber_climb
add constraint check_unique2 unique (climber_id, climb_id, club_id);
end if;
end $$;


do $$
begin
if not exists (
	select 1
	from information_schema.table_constraints
	where 
		table_name = 'climber_equipment'
		and constraint_name = 'check_unique4'
) 
then 
alter table training_data.climber_equipment
add constraint check_unique4 unique (equipment_id, climber_id);
end if;
end $$;


-- added not null constraints

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'club'
		and column_name = 'email'
		and is_nullable  = 'no'
) 
then
alter table training_data.club
alter column email set not null;
end if;
end $$;


do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'club'
		and column_name = 'telephone_number'
		and is_nullable  = 'no'
) 
then
alter table training_data.club
alter column telephone_number set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'route'
		and column_name = 'name'
		and is_nullable  = 'no'
) 
then
alter table training_data.route
alter column name set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'route'
		and column_name = 'difficulty_level'
		and is_nullable  = 'no'
) 
then
alter table training_data.route
alter column difficulty_level set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'mountain'
		and column_name = 'name'
		and is_nullable  = 'no'
) 
then
alter table training_data.mountain
alter column name set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'donation'
		and column_name = 'date_sponsored'
		and is_nullable  = 'no'
) 
then
alter table training_data.donation
alter column date_sponsored set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'address'
		and column_name = 'country'
		and is_nullable  = 'no'
) 
then
alter table training_data.address
alter column country set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'address'
		and column_name = 'city'
		and is_nullable  = 'no'
) 
then
alter table training_data.address
alter column city set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'address'
		and column_name = 'street'
		and is_nullable  = 'no'
) 
then
alter table training_data.address
alter column street set not null;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'address'
		and column_name = 'house_number'
		and is_nullable  = 'no'
) 
then
alter table training_data.address
alter column house_number set not null;
end if;
end $$;


--added default values

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'climber'
		and column_name = 'experience_level'
		and column_default is not null
) 
then
alter table training_data.climber
alter column experience_level set default 'beginner';
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'donation'
		and column_name = 'date_sponsored'
		and column_default is not null
) 
then
alter table training_data.donation
alter column date_sponsored set default current_date;
end if;
end $$;

do $$
begin
if not exists (
	select 1
	from information_schema.columns
	where 
		table_name = 'route'
		and column_name = 'name'
		and column_default is not null
) 
then
alter table training_data.route
alter column name set default 'no name';
end if;
end $$;



--insert sample rows in the tables

--1
with club_data as (
	select 'Climbs In Seconds' as name, 'Poland' as location, '+375174551789' as telephone_number, 'climbinseconds@gmail.com' as email, 'Mountain climbing club focused on promoting outdoor/indoor adventures and fostering a community of climbers. The club organizes various climbing events and training sessions while seeking sponsorship to support both individual climbers and overall club activities.' as description
	union all
	select 'x' as name, 'Lithuania' as location, '+37066171780' as telephone_number, 'x@gmail.com' as email, 'information about club' as description
)
insert into training_data.club (name, location, telephone_number, email, description)
select name, location, telephone_number, email, description
from club_data as cd
where not exists (
	select 1 from training_data.club where upper(name) = upper(cd.name)
)
returning *;



with sponsor_data as (
	select 'John' as name, 'Canon' as surname, 'individual' as sponsor_type
	union all
	select 'Microsoft' as name, null as surname, 'corporate' as sponsor_type
)
insert into training_data.sponsor (name, surname, sponsor_type)
select name, surname, sponsor_type
from sponsor_data as sd
where not exists (
	select 1 
	from training_data.sponsor 
	where upper(coalesce(name, '') || ' ' || coalesce(surname, '')) = upper(coalesce(sd.name, '') || ' ' || coalesce(sd.surname, ''))
)
returning *;


with address_data as (
	select 'Lithuania' as country, 'Vilnius' as city, 'Tuskulenu' as street, '41' as house_number, 4 as flat_number
	union all
	select 'Poland' as country, 'Krakow' as city, 'Krakowska' as street, '4a' as house_number, null as flat_number
)
insert into training_data.address (country, city, street, house_number, flat_number)
select country, city, street, house_number, flat_number
from address_data as ad
where not exists (
	select 1 
	from training_data.address 
	where upper(country) = upper(ad.country) and upper(city) = upper(ad.city) and upper(street) = upper(ad.street) and house_number = ad.house_number
)
returning *;


with equipment_data as (
	select  'Petzl' as name, 'rope' as equipment_type
	union all
	select 'Black Diamond' as name, 'climbing helmet' as equipment_type
)
insert into training_data.equipment (name, equipment_type)
select name, equipment_type
from  equipment_data as ed 
where not exists (
	select 1 
	from training_data.equipment
	where upper(name) = upper(ed.name) and upper(equipment_type) = upper(ed.equipment_type)
)
returning *;


with climb_data as (
	select 'Alpine Challenge' as name
	union all
	select 'Zakopane Morske Oko' as name
)
insert into training_data.climb (name)
select name
from climb_data as cld 
where not exists (
	select 1 
	from training_data.climb
	where upper(name) = upper(cld.name)
)
returning *;


with location_data as (
	select 'Poland' as country, 'x' as region
	union all
	select 'Himalai' as country, 'z' as region
)
insert into training_data.location (country, region)
select country, region
from  location_data as ld  
where not exists (
	select 1 
	from training_data.location
	where upper(country) = upper(ld.country) and upper(region) = upper(ld.region)
)
returning *;

--2
with climber_data as (
	select 'Jon' as name, 'Jonovich' as surname, 'advanced' as experience_level, a.address_id
	from training_data.address a
	where upper(country) = upper('Poland') and upper(city) = upper('Krakow') and upper(street) = upper('Krakowska')
	union all
	select 'Laimis' as name, 'Lietutis' as surname, 'intermediate' as experience_level, a.address_id
	from training_data.address a
	where upper(country) = upper('Lithuania') and upper(city) = upper('Vilnius') and upper(street) = upper('Tuskulenu')
)
insert into training_data.climber (name, surname, experience_level, address_id)
select name, surname, experience_level, address_id
from climber_data as clda
where not exists (
	select 1 
	from training_data.climber
	where upper(name) = upper(clda.name) and upper(surname) = upper(clda.surname)
)
returning *;


with weather_data as (
	select climb_id, 18.5 as temperature, 5.2 as wind_speed, false as precipation
	from training_data.climb
	where upper(name) = upper('Zakopane Morske Oko')
	union all
	select climb_id, 7.0 as temperature, 6.3 as wind_speed, true as precipation
	from training_data.climb
	where upper(name) = upper('Alpine Challenge')
)
insert into training_data.weather (climb_id, temperature, wind_speed, precipation)
select climb_id, temperature, wind_speed, precipation
from weather_data as wd 
where not exists (
	select 1 
	from training_data.weather
	where climb_id = wd.climb_id
)
returning *;


with mountain_data as (
	select 'Alpine' as name, 4807 as height, location_id
	from training_data.location 
	where upper(country) = 'HIMALAI'
	union all
	select 'Tatras' as name, 2655 as height, location_id
	from training_data.location 
	where upper(country) = 'POLAND'
)
insert into training_data.mountain (name, height, location_id)
select name, height, location_id
from  mountain_data as md  
where not exists (
	select 1 
	from training_data.mountain
	where upper(name) = upper(md.name)
)
returning *;


with climb_schedule_data as (
	select climb_id, current_date as begin_date, current_date as end_date
	from training_data.climb 
	where upper(name) = 'ALPINE CHALLENGE'
	union all
	select climb_id, current_date as begin_date, current_date as end_date
	from training_data.climb 
	where upper(name) = 'ZAKOPANE MORSKE OKO'
)
insert into training_data.climb_schedule (climb_id, begin_date, end_date)
select climb_id, begin_date, end_date
from climb_schedule_data as csd  
where not exists (
	select 1 
	from training_data.climb_schedule
	where climb_id = csd.climb_id
)
returning *;


--3

with 
	club_data as (
	select club_id
	from training_data.club c
	where upper(name) = 'CLIMBS IN SECONDS'
),
	sponsor_data as (
	select sponsor_id
	from training_data.sponsor s 
	where upper(name) = 'JOHN' and upper(surname) = 'CANON'
),
	climber_data as (
	select climber_id
	from training_data.climber c 
	where upper(name) is null
)
insert into training_data.donation (club_id, sponsor_id, climber_id, contribution)
select club_id, sponsor_id, null as climber_id, 500.00 as contribution
from (
	select *
	from club_data
		cross join sponsor_data
) as new_table
where not exists (
	select 1 
	from training_data.donation 
	where sponsor_id = new_table.sponsor_id and club_id = new_table.club_id 
)
returning *;


with 
	club_data as (
	select club_id
	from training_data.club c
	where upper(name) is null
),
	sponsor_data as (
	select sponsor_id
	from training_data.sponsor s 
	where upper(name) = 'JOHN' and upper(surname) = 'CANON'
),
	climber_data as (
	select climber_id
	from training_data.climber c 
	where upper(name) = 'JON' and upper(surname) = 'JONOVICH'
)
insert into training_data.donation (club_id, sponsor_id, climber_id, contribution)
select null as club_id, sponsor_id,climber_id, 1000.00 as contribution
from (
	select *
	from climber_data
		cross join sponsor_data
) as new_table
where not exists (
	select 1 
	from training_data.donation 
	where sponsor_id = new_table.sponsor_id and climber_id = new_table.climber_id 
)
returning *;

with 
	climber_data as (
	select climber_id
	from training_data.climber c
	where upper(name) = 'LAIMIS' and upper(surname) = 'LIETUTIS'
),
	equipment_data as (
	select equipment_id
	from training_data.equipment eq 
	where upper(name) = 'BLACK DIAMOND' and upper(equipment_type) = 'CLIMBING HELMET'
)
insert into training_data.climber_equipment (climber_id, equipment_id)
select climber_id, equipment_id
from (
	select *
	from climber_data
		cross join equipment_data
) as new_table
where not exists (
	select 1 
	from training_data.climber_equipment
	where climber_id = new_table.climber_id and equipment_id = new_table.equipment_id 
)
returning *;


with 
	climber_data as (
	select climber_id
	from training_data.climber c
	where upper(name) = 'LAIMIS' and upper(surname) = 'LIETUTIS'
),
	equipment_data as (
	select equipment_id
	from training_data.equipment eq 
	where upper(name) = 'PETZL' and upper(equipment_type) = 'ROPE'
)
insert into training_data.climber_equipment (climber_id, equipment_id)
select climber_id, equipment_id
from (
	select *
	from climber_data
		cross join equipment_data
) as new_table
where not exists (
	select 1 
	from training_data.climber_equipment
	where climber_id = new_table.climber_id and equipment_id = new_table.equipment_id 
)
returning *;


with 
	club_data as (
	select club_id
	from training_data.club c
	where upper(name) = 'CLIMBS IN SECONDS'
),
	climb_data as (
	select climb_id
	from training_data.climb cl 
	where upper(name) = 'ALPINE CHALLENGE'
),
	climber_data as (
	select climber_id
	from training_data.climber c 
	where upper(name) = 'LAIMIS' and upper(surname) = 'LIETUTIS'
)
insert into training_data.climber_climb (club_id, climb_id, climber_id, climb_type)
select club_id, climb_id, climber_id, 'outside' as climb_type
from (
	select *
	from club_data
		cross join climber_data
		cross join climb_data
) as new_table
where not exists (
	select 1 
	from training_data.climber_climb
	where climber_id = new_table.climber_id and club_id = new_table.club_id and climb_id = new_table.climb_id
)
returning *;

with 
	club_data as (
	select club_id
	from training_data.club c
	where upper(name) = 'CLIMBS IN SECONDS'
),
	climb_data as (
	select climb_id
	from training_data.climb cl 
	where upper(name) = 'ZAKOPANE MORSKE OKO'
),
	climber_data as (
	select climber_id
	from training_data.climber c 
	where upper(name) = 'LAIMIS' and upper(surname) = 'LIETUTIS'
)
insert into training_data.climber_climb (club_id, climb_id, climber_id, climb_type)
select club_id, climb_id, climber_id, 'outside' as climb_type
from (
	select *
	from club_data
		cross join climber_data
		cross join climb_data
) as new_table
where not exists (
	select 1 
	from training_data.climber_climb
	where climber_id = new_table.climber_id and club_id = new_table.club_id and climb_id = new_table.climb_id
)
returning *;




with route_data as (
	select 'Best' as name, 'Expert' as difficulty_level, mountain_id
	from training_data.mountain 
	where upper(name) = 'ALPINE'
	union all
	select 'EVERY DAY' as name, 'Easy' as difficulty_level, mountain_id
	from training_data.mountain 
	where upper(name) = 'ALPINE'
)
insert into training_data.route (name, difficulty_level, mountain_id)
select name, difficulty_level, mountain_id
from  route_data as rd  
where not exists (
	select 1 
	from training_data.route
	where upper(name) = upper(rd.name)
)
returning *;


--4

with 
	club_data as (
	select club_id
	from training_data.club c
	where upper(name) = 'CLIMBS IN SECONDS'
),
	climb_data as (
	select climb_id
	from training_data.climb cl 
	where upper(name) = 'ZAKOPANE MORSKE OKO'
),
	route_data as (
	select route_id
	from training_data.route r 
	where upper(name) = 'BEST'
)
insert into training_data.climb_route (club_id, climb_id, route_id)
select club_id, climb_id, route_id
from (
	select *
	from club_data
		cross join route_data
		cross join climb_data
) as new_table
where not exists (
	select 1 
	from training_data.climb_route
	where route_id = new_table.route_id and club_id = new_table.club_id and climb_id = new_table.climb_id
)
returning *;


with 
	club_data as (
	select club_id
	from training_data.club c
	where upper(name) = 'CLIMBS IN SECONDS'
),
	climb_data as (
	select climb_id
	from training_data.climb cl 
	where upper(name) = 'ZAKOPANE MORSKE OKO'
),
	route_data as (
	select route_id
	from training_data.route r 
	where upper(name) = 'EVERY DAY'
)
insert into training_data.climb_route (club_id, climb_id, route_id)
select club_id, climb_id, route_id
from (
	select *
	from club_data
		cross join route_data
		cross join climb_data
) as new_table
where not exists (
	select 1 
	from training_data.climb_route
	where route_id = new_table.route_id and club_id = new_table.club_id and climb_id = new_table.climb_id
)
returning *;




--I added new columns in all tables record_ts

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'address' and column_name = 'record_ts') 
then
alter table training_data.address
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'climb' and column_name = 'record_ts') 
then
alter table training_data.climb
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'climb_route' and column_name = 'record_ts') 
then
alter table training_data.climb_route
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'climb_schedule' and column_name = 'record_ts') 
then
alter table training_data.climb_schedule
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'climber' and column_name = 'record_ts') 
then
alter table training_data.climber
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'climber_climb' and column_name = 'record_ts') 
then
alter table training_data.climber_climb
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'climber_equipment' and column_name = 'record_ts') 
then
alter table training_data.climber_equipment
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'club' and column_name = 'record_ts') 
then
alter table training_data.club
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'donation' and column_name = 'record_ts') 
then
alter table training_data.donation
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'equipment' and column_name = 'record_ts') 
then
alter table training_data.equipment
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'location' and column_name = 'record_ts') 
then
alter table training_data.location
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'mountain' and column_name = 'record_ts') 
then
alter table training_data.mountain
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'route' and column_name = 'record_ts') 
then
alter table training_data.route
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'sponsor' and column_name = 'record_ts') 
then
alter table training_data.sponsor
add column record_ts date not null default current_date;
end if;
end $$;

do $$
begin
if not exists (select 1 from information_schema.columns 
where table_name = 'weather' and column_name = 'record_ts') 
then
alter table training_data.weather
add column record_ts date not null default current_date;
end if;
end $$;


