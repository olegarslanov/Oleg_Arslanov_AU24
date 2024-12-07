--Tasks: applying view and functions

--Note:
--Please pay attention that your code must be reusable and rerunnable and executes without errors.
--Don't hardcode IDs
--Add RAISE EXCEPTION to identify errors
--Don't forget to check for duplicates, ensure that the object has not already been created
--Check that the function is run correctly and returns the desired result. Don't forget about optional parameters



--Task 1. Create a view
--Create a view called 'sales_revenue_by_category_qtr' that shows the film category and total sales revenue for the current quarter and year.
--The view should only display categories with at least one sale in the current quarter. 
--Note: when the next quarter begins, it will be considered as the current quarter.


-- this view is for fixed date 2017-01-24 because in DB we dont have data for current date
create or replace view sales_revenue_by_category_qtr as
select c.name as film_category, sum(amount) as revenue
from public.category c
 	inner join public.film_category fc on c.category_id = fc.category_id
 	inner join public.film f on fc.film_id = f.film_id
 	inner join public.inventory i on i.film_id = f.film_id
 	inner join public.rental r on r.inventory_id = i.inventory_id
 	inner join public.payment p on p.rental_id = r.rental_id
-- zdes ja ispolzuju filtr dlja togo ctob po current_date poluchit period vremeni chetvert goda. Ispolzuja filtr where
-- ja vybyraju period between nacalnoj datoj and datoj konca opredelennoj chetverti. Chtob poluchit datu nachala ja ispolzuju 
-- extract iz daty po mesiacu, togda prichisliaju mesiac k opredelennoj chetverti i s pomoschju to_date formiruju datu nachalo chetverti.
-- pohozee delaju s datoj konca chetverti. V konce koncov filtr dast vozmozhnost pokazat tolko filmy s opredelenoj chetverti goda  	
where
	p.payment_date between 
	case
		when extract(month from date '2017-01-24') between 10 and 12 then to_date(extract(year from date '2017-01-24') || '-10-01', 'yyyy-mm-dd') 
		when extract(month from date '2017-01-24') between 07 and 09 then to_date(extract(year from date '2017-01-24') || '-07-01', 'yyyy-mm-dd')
		when extract(month from date '2017-01-24') between 04 and 06 then to_date(extract(year from date '2017-01-24') || '-04-01', 'yyyy-mm-dd')
		else to_date(extract(year from date '2017-01-24') || '-01-01', 'yyyy-mm-dd')
	end and
	case
		when extract(month from date '2017-01-24') between 10 and 12 then to_date(extract(year from date '2017-01-24') || '-12-31', 'yyyy-mm-dd')
		when extract(month from date '2017-01-24') between 07 and 09 then to_date(extract(year from date '2017-01-24') || '-09-30', 'yyyy-mm-dd')
		when extract(month from date '2017-01-24') between 04 and 06 then to_date(extract(year from date '2017-01-24') || '-06-30', 'yyyy-mm-dd')
		else to_date(extract(year from date '2017-01-24') || '-03-31', 'yyyy-mm-dd')
	end
group by c.name
having count(amount)>=1;


-- I did that function for current date, but it not return rows ... I think because in previous HW I dont added data at category and film_category tables
create or replace view sales_revenue_by_category_qtr as
select 
c.name as film_category, 
sum(p.amount) as revenue
from 
public.category c
inner join public.film_category fc on c.category_id = fc.category_id
inner join public.film f on fc.film_id = f.film_id
inner join public.inventory i on i.film_id = f.film_id
inner join public.rental r on r.inventory_id = i.inventory_id
inner join public.payment p on p.rental_id = r.rental_id
--zdes ja ispolzuju filtr dlja togo ctob po current_date poluchit period vremeni chetvert goda. Ispolzuja filtr where
-- ja vybyraju period between nacalnoj datoj and datoj konca opredelennoj chetverti. Chtob poluchit datu nachala ja ispolzuju 
-- extract iz daty po mesiacu, togda prichisliaju mesiac k opredelennoj chetverti i s pomoschju to_date formiruju datu nachalo chetverti.
-- pohozee delaju s datoj konca chetverti. V konce koncov filtr dast vozmozhnost pokazat tolko filmy s opredelenoj chetverti goda 
where 
p.payment_date between 
case 
	when extract(month from current_date) between 10 and 12 then to_date(extract(year from current_date) || '-10-01', 'yyyy-mm-dd')
	when extract(month from current_date) between 07 and 09 then to_date(extract(year from current_date) || '-07-01', 'yyyy-mm-dd')
	when extract(month from current_date) between 04 and 06 then to_date(extract(year from current_date) || '-04-01', 'yyyy-mm-dd')
	else to_date(extract(year from current_date) || '-01-01', 'yyyy-mm-dd')
end and 
case 
	when extract(month from current_date) between 10 and 12 then to_date(extract(year from current_date) || '-12-31', 'yyyy-mm-dd')
	when extract(month from current_date) between 07 and 09 then to_date(extract(year from current_date) || '-09-30', 'yyyy-mm-dd')
	when extract(month from current_date) between 04 and 06 then to_date(extract(year from current_date) || '-06-30', 'yyyy-mm-dd')
	else to_date(extract(year from current_date) || '-03-31', 'yyyy-mm-dd')
end
group by 
c.name;

select * from sales_revenue_by_category_qtr;


--Task 2. Create a query language functions
--Create a query language function called 'get_sales_revenue_by_category_qtr' that accepts one parameter representing the current quarter
--and year and returns the same result as the 'sales_revenue_by_category_qtr' view.

--create function with one input parametr
create or replace function get_sales_revenue_by_category_qtr (in input_date date)
--ustanavlivaem format vozvrata kak tablicu
returns table (film_category text, revenue numeric)
as $$
select 
	c.name as film_category,
	sum(amount) as revenue
from 
	public.category c
 	inner join public.film_category fc on c.category_id = fc.category_id
 	inner join public.film f on fc.film_id = f.film_id
 	inner join public.inventory i on i.film_id = f.film_id
 	inner join public.rental r on r.inventory_id = i.inventory_id
 	inner join public.payment p on p.rental_id = r.rental_id
where 
p.payment_date between
	case 
		when extract(month from input_date) between 10 and 12 then to_date(extract(year from input_date) || '-10-01', 'yyyy-mm-dd')
		when extract(month from input_date) between 07 and 09 then to_date(extract(year from input_date) || '-07-01', 'yyyy-mm-dd')
		when extract(month from input_date) between 04 and 06 then to_date(extract(year from input_date) || '-04-01', 'yyyy-mm-dd')
		else to_date(extract(year from input_date) || '-01-01', 'yyyy-mm-dd')
	end and 
	case 
		when extract(month from input_date) between 10 and 12 then to_date(extract(year from input_date) || '-12-31', 'yyyy-mm-dd')
		when extract(month from input_date) between 07 and 09 then to_date(extract(year from input_date) || '-09-30', 'yyyy-mm-dd')
		when extract(month from input_date) between 04 and 06 then to_date(extract(year from input_date) || '-06-30', 'yyyy-mm-dd')
		else to_date(extract(year from input_date) || '-03-31', 'yyyy-mm-dd')
	end
group by c.name
having count(amount) >=1
$$ 
language sql;


select * from get_sales_revenue_by_category_qtr ('2017-01-24')


--Task 3. Create procedure language functions
--Create a function that takes a country as an input parameter and returns the most popular film in that specific country. 
--The function should format the result set as follows:
--Query (example): select * from core.most_popular_films_by_countries(array['Afghanistan','Brazil','United States’]);


--added a RAISE EXCEPTION about an invalid parameter 
create or replace function get_most_popular_film_by_countries (country_names text[])
returns table (country text, film text, rating mpaa_rating, language bpchar(20), length int2, release_year year)
language plpgsql
as $$
declare
	countryX text;
begin
	for countryX in 
		select unnest(country_names)
	loop
		if not exists (select 1 from public.country pc where upper(pc.country) = upper(countryX)) then
			Raise exception 'Country must be inside dvdrental db. Calculating is close!';
		end if;
	end loop;
return query
with count_rental_film as (
	select co.country, f.title as film, f.rating, l.name, f.length, f.release_year, count(r.rental_id) AS rental_count
	from public.rental r
	inner join public.inventory i on r.inventory_id = i.inventory_id
	inner join public.film f on f.film_id = i.film_id
	inner join public.customer c on c.customer_id = r.customer_id 
	inner join public.address a on a.address_id = c.address_id 
	inner join public.city ci on ci.city_id = a.city_id 
	inner join public.country co on co.country_id = ci.country_id
	inner join public.language l on l.language_id = f.language_id
	where upper(co.country) = any (select upper(unnest(country_names)))
	group by co.country, f.title, f.rating, l.name, f.length, f.release_year
)
select crf.country, crf.film, crf.rating, crf.name as language, crf.length, crf.release_year
from count_rental_film as crf
where crf.rental_count = (
	select max(crf2.rental_count)
	from count_rental_film as crf2
	where crf2.country = crf.country
);
end;
$$;

select * 
from get_most_popular_film_by_countries(array['Russian Federation', 'Brazil', 'United States']);



-- optional query with only one parametr
create or replace function get_most_popular_film_by_country (country_name text)
returns table (title text, rental_count bigint)
language plpgsql
as $function$
begin
return query
with count_rental_film as (
select f.title , count(r.rental_id) AS rental_count
from public.rental r
inner join public.inventory i on r.inventory_id = i.inventory_id
inner join public.film f on f.film_id = i.film_id
inner join public.customer c on c.customer_id = r.customer_id 
inner join public.address a on a.address_id = c.address_id 
inner join public.city ci on ci.city_id = a.city_id 
inner join public.country co on co.country_id = ci.country_id
where upper(country) = upper(country_name)
group by f.title
)
select crf.title, crf.rental_count
from count_rental_film as crf
where crf.rental_count  = (
select max(crf2.rental_count)
from count_rental_film as crf2
);
end;
$function$;

select *
from get_most_popular_film_by_country ('Brazil');


--Task 4. Create procedure language functions
--Create a function that generates a list of movies available in stock based on a partial title match (e.g., movies containing the word 'love' in their title). 
--The titles of these movies are formatted as '%...%', and if a movie with the specified title is not in stock, return a message indicating that it was not found.
--The function should produce the result set in the following format (note: the 'row_num' field is an automatically generated counter field, starting from 1 and 
--incrementing for each entry, e.g., 1, 2, ..., 100, 101, ...).
--Query (example):select * from core.films_in_stock_by_title('%love%’);


create or replace function get_films_in_stock_by_title(title_name text)
returns table (row_num bigint,film_title text, language bpchar(20), customer_name text, rental_date timestamptz)
language plpgsql
as $function$
begin
return query
select ROW_NUMBER() OVER () AS row_num, title as film_title, l.name as language, c.first_name || ' ' || c.last_name as customer_name, r.rental_date 
from public.rental r
inner join public.inventory i on r.inventory_id = i.inventory_id
inner join public.film f on f.film_id = i.film_id
inner join public.customer c on c.customer_id = r.customer_id 
inner join public.language l on l.language_id = f.language_id
where upper(title) like upper(title_name);
if not found then 
	raise exception 'Film with title % does not exists! Ya ya das is fantastic', title_name;
end if;
end;
$function$;


select * 
from get_films_in_stock_by_title('%love%');

-- optional with loops (I added here for remember, maybe it helps me in future)
create or replace function get_films_in_stock_by_title2(title_name text)
returns table (
	row_num bigint,
	film_title text,
	language bpchar(20),
	customer_name text,
	rental_date timestamptz
)
language plpgsql
as $function$
declare
	counter bigint :=0; --counter for row_num
begin
    for film_title, language, customer_name, rental_date -- for ... in loop for all rows
	IN
        select 
            f.title as film_title, 
            l.name as language, 
            c.first_name || ' ' || c.last_name AS customer_name, 
            r.rental_date
        from public.rental r
		inner join public.inventory i on r.inventory_id = i.inventory_id
		inner join public.film f on f.film_id = i.film_id
		inner join public.customer c on c.customer_id = r.customer_id 
		inner join public.language l on l.language_id = f.language_id
		where upper(title) like upper(title_name)
    loop
        counter := counter + 1; -- counter +1
        row_num := counter;
        return next; -- returns current row
    end loop;
-- if dont find nothing
if not found then 
	raise exception 'Film with title % does not exists! Ya ya das is fantastic', title_name;
end if;
end;
$function$;

select * 
from get_films_in_stock_by_title2('%love%');

--drop function get_films_in_stock_by_title(text);


--Task 5. Create procedure language functions
--Create a procedure language function called 'new_movie' that takes a movie title as a parameter and inserts a new movie with the given title in the film table.
--The function should generate a new unique film ID, set the rental rate to 4.99, the rental duration to three days, the replacement cost to 19.99. 
--The release year and language are optional and by default should be current year and Klingon respectively. The function should also verify that the language exists
-- in the 'language' table. Then, ensure that no such function has been created before; if so, replace it.


-- I dont understand for what we need add 'Klingon' language if we dont have it in language table ... 
-- if we insert no language we got raise exception and can not add to db 


create or replace function new_movie (
	movie_title text,
	movie_release_year year default extract(year from current_date)::integer,
	movie_language_name bpchar(20) default 'Klingon'
	)
returns void
language plpgsql
as $function$
begin
-- check if film exists in the DB
    if exists (select 1 from public.film where upper(title) = upper(movie_title)
	) then
		raise exception 'Film "%" already exists in the DB!', movie_title;
    end if;
-- check if language name in language table if not add
	if not exists (select 1 from public.language where upper(name) = upper(movie_language_name))
	then	
		insert into public.language (name)
		values ('klingon');	
	end if;
-- esli est jazyk v language dobavliaem v film, a esli netu to dobavliaem klingon id iz language table
	if exists 
		(select 1 from public.language where upper(name) = upper(movie_language_name))
	then
		insert into public.film (title, release_year, language_id)
		values (movie_title, movie_release_year, (select language_id from public.language where upper(name) = upper(movie_language_name)));
	else 
		insert into public.film (title, release_year, language_id)
		values (movie_title, movie_release_year, (select language_id from public.language where upper(name) = 'KLINGON'));
	end if; 	
end;
$function$;


--dlja proverki ... u menja rabotaet
select new_movie ('Test143', 2010, 'lish');
select new_movie ('Test140', 2010);
select new_movie ('Test130');
select new_movie ('Test123', 2020, 'english');
select new_movie (movie_title:='Test134', movie_language_name:='english');
select * from public.film








 
 
 
 
 
 
 
 
 
 
 
