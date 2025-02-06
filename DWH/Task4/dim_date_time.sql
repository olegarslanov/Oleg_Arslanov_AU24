--I. DATE tables--

-- create table of date 
create table if not exists public.DIM_DATES (
	date_sur_id bigint primary key not null,
	calendar_date date not null,
	day_of_week varchar not null,
	day integer not null check (day > 0),
	month integer not null check (month between 1 and 12),
	quarter integer not null check (quarter between 1 and 4),
	year integer not null check (year >= 0)
);

--1 option data insert into table

--creat function for create date table 
create or replace function creates_dates_table2 (date_start date, date_end date, interval_day interval)
returns table (
    date_sur_id bigint,
    calendar_date date,
    day_of_week varchar,
    day integer,
    month integer,
    quarter integer,
    year integer
) as $$
begin
    -- insert data into the table (table must be created before)
    insert into public.DIM_DATES (date_sur_id, calendar_date, day_of_week, day, month, quarter, year)
    select 
        cast(extract(year from dates) * 10000 + extract(month from dates) * 100 + extract(day from dates) as bigint) as date_sur_id,
        dates as calendar_date,
        trim(to_char(dates, 'day')) as day_of_week,  -- trim remove spaces
        extract(day from dates) as day,
        extract(month from dates) as month,
        extract(quarter from dates) as quarter,
        extract(year from dates) as year
    from generate_series(date_start, date_end, interval_day) as dates
    where not exists (
        select 1 from public.DIM_DATES d where d.calendar_date = dates
    );

    -- return values that were inserted now
    return query
    select d.date_sur_id, d.calendar_date, d.day_of_week, d.day, d.month, d.quarter, d.year 
    from public.DIM_DATES d;
end;
$$ language plpgsql;


--call function with arguments
select * from creates_dates_table2('2000-01-01','2029-12-31', '1 day');



--2 option data insert into table

--insert data to table
insert into public.DIM_DATES (date_sur_id, calendar_date, day_of_week, day, month, quarter, year)
select 
	cast(extract (year from dates) *10000 + extract(month from dates) *100 + extract(day from dates) as bigint) as date_sur_id,
	dates as calendar_date,
	to_char(dates, 'day') as day_of_week,
	extract(day from dates) as day,
	extract(month from dates) as month,
	extract (quarter from dates) as quarter,
	extract (year from dates) as year
from generate_series('2000-01-01'::date, '2029-12-31'::date, '1 day'::interval) as dates
where not exists (
select 1 from public.dim_dates where calendar_date = dates
);


--To check what inside table
select * from public.dim_dates;




----------------------------------------------------------------------------------------------------------------------------------
--II. TIME tables--

-- create table of time 
create table if not exists public.DIM_TIME (
	time_sur_id bigint primary key not null,
	calendar_time time not null,
	hour integer not null check (hour between 0 and 23) ,
	minute integer not null check (minute between 0 and 59)
);


-- Create or replace function to create time table
create or replace function creates_time_table (
    time_start time, 
    time_end time, 
    interval_minute interval
)
returns table (
    time_sur_id bigint,
    calendar_time time,
    hour integer,
    minute integer
) as $$

-- Begin function process
begin
    -- Insert data into the table DIM_TIME (Ensure table exists before)
    insert into public.DIM_TIME (time_sur_id, calendar_time, hour, minute)
    select 
        cast(extract(hour from time_ ) * 100 + extract(minute from time_) as bigint) as time_sur_id,
        time_::time as calendar_time,  -- Convert back to 'time'
        extract(hour from time_) as hour,
        extract(minute from time_) as minute
    from generate_series(
            '2000-01-01 00:00:00'::timestamp + (time_start - '00:00'::time), --here (time_start - '00:00'::time) we made interval, so we can add it to timestam value and get needed time from
            '2000-01-01 00:00:00'::timestamp + (time_end - '00:00'::time), 
            interval_minute
        ) as time_
    where not exists (
        select 1 from public.DIM_TIME t where t.calendar_time = time_::time
    );

    -- Return the rows inserted into the table DIM_TIME
    return query
    select t.time_sur_id, t.calendar_time, t.hour, t.minute 
    from public.DIM_TIME t
    where t.calendar_time between time_start and time_end;
end;
$$ language plpgsql;


--call function with arguments
select * from creates_time_table('00:00'::time,'23:59'::time, '1 minute'::interval);




--To check what inside table
select * from public.dim_time;


--truncate table public.dim_dates;
--DROP TABLE public.DIM_DATES;
--DROP TABLE public.DIM_TIME;
