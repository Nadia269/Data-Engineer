-- Active: 1721872569477@@127.0.0.1@5432@sfpolice
create database sfpolice;

create table police_incident_reports (
    pd_id BIGINT,
    IncidentNum VARCHAR(10),
    "Incident Code" VARCHAR(10),
    Category VARCHAR(50),
    Descript VARCHAR(100),
    DayOfWeek VARCHAR(10),
    Date DATE,
    Time TIME,
    PdDistrict VARCHAR(10),
    Resolution VARCHAR(50),
    Address VARCHAR(100),
    X NUMERIC(9, 6),
    Y NUMERIC(9, 6),
    location VARCHAR(55),
    "SF Find Neighborhoods 2 2" FLOAT,
    "Current Police Districts 2 2" INT,
    "Current Supervisor Districts 2 2" INT,
    "Analysis Neighborhoods 2 2" INT,
    "DELETE - Fire Prevention Districts 2 2" INT,
    "DELETE - Police Districts 2 2" INT,
    "DELETE - Supervisor Districts 2 2" INT,
    "DELETE - Zip Codes 2 2" INT,
    "DELETE - Neighborhoods 2 2" INT,
    "DELETE - 2017 Fix It Zones 2 2" INT,
    "Civic Center Harm Reduction Project Boundary 2 2" INT,
    "Fix It Zones as of 2017-11-06 2 2" INT,
    "DELETE - HSOC Zones 2 2" INT,
    "Fix It Zones as of 2018-02-07 2 2" INT,
    "CBD, BID and GBD Boundaries as of 2017 2 2" INT,
    "Areas of Vulnerability, 2016 2 2" INT,
    "Central Market/Tenderloin Boundary 2 2" INT,
    "Central Market/Tenderloin Boundary Polygon - Updated 2 2" INT,
    "HSOC Zones as of 2018-06-05 2 2" INT,
    "OWED Public Spaces 2 2" INT,
    "Neighborhoods 2" INT
);

COPY police_incident_reports
FROM 'E:\New Downloads here\Csv/Police_Department_Incident_Reports__Historical_2003_to_May_2018_20240805.csv' DELIMITER ',' CSV HEADER QUOTE '"';

SELECT
    pd_id,
    incidentnum,
    "Incident Code",
    category,
    descript,
    dayofweek,
    date,
    "time",
    pddistrict,
    resolution,
    address,
    x,
    y,
    location
    -- 	"SF Find Neighborhoods 2 2", 
    -- 	"Current Police Districts 2 2", 
    -- 	"Current Supervisor Districts 2 2", 
    -- 	"Analysis Neighborhoods 2 2"
    -- 	"Civic Center Harm Reduction Project Boundary 2 2", 
    -- 	"Fix It Zones as of 2017-11-06 2 2", 
    -- 	"Fix It Zones as of 2018-02-07 2 2", 
    -- 	"CBD, BID and GBD Boundaries as of 2017 2 2", 
    -- 	"Areas of Vulnerability, 2016 2 2", 
    -- 	"Central Market/Tenderloin Boundary 2 2", 
    -- 	"Central Market/Tenderloin Boundary Polygon - Updated 2 2", 
    -- 	"HSOC Zones as of 2018-06-05 2 2", 
    -- 	"OWED Public Spaces 2 2", 
    -- 	"Neighborhoods 2"
FROM public.police_incident_reports
LIMIT 100;

select date_part('year', max(date)), date_part('year', min(date))
FROM public.police_incident_reports;

SELECT COUNT(DISTINCT pddistrict) FROM police_incident_reports;

select ('Neighborhoods 2') FROM police_incident_reports;

select ('Neighborhoods 2'), count(pd_id) as count
from police_incident_reports
GROUP BY
    1
ORDER BY count(*);

select Max(s.count) As Max, Min(s.count) AS Min, AVG(s.count) As Avg
from (
        select ('Neighborhoods 2'), count(*) as count
        from police_incident_reports
        GROUP BY
            "Neighborhoods 2"
        ORDER BY count(*)
    ) as s;

create function average_no_of_incidents()
returns int 
as $$
declare 
  average int :=0;
begin 
  select Avg(s.count) into average
 from(
  select ('Neighborhoods 2'),count(*) as count from police_incident_reports GROUP BY "Neighborhoods 2" ORDER BY count(*)
) as s;
return average;
end;
$$ language PLPGSQL;

select average_no_of_incidents ()

select
    "Neighborhoods 2",
    count(*),
    case
        when count(*) > average_no_of_incidents () then 'high'
        else 'low'
    end as nadia
from police_incident_reports
GROUP BY
    "Neighborhoods 2"
ORDER BY count(*);

select * from public.police_incident_reports limit 5;

select category, count(incidentnum)
from police_incident_reports
GROUP BY
    category;

select category, pddistrict, count(incidentnum)
from police_incident_reports
GROUP BY
    category,
    pddistrict;
-- window function
select *, count(incidentnum) over () from police_incident_reports;

select category, count(incidentnum) over (
        PARTITION BY
            category
    )
from police_incident_reports;

Select
    category,
    pddistrict,
    descript,
    count(incidentnum) over ()
from police_incident_reports;

--  count number of accidents for the category only
select category, pddistrict, count(incidentnum) over (
        PARTITION BY
            category
    )
from police_incident_reports;

Select
    category,
    pddistrict,
    descript,
    count(incidentnum) over (
        partition by
            category
    )
from police_incident_reports;

select category, count(incidentnum) over (
        PARTITION BY
            category
    )
from police_incident_reports;

select DISTINCT
    category,
    count(incidentnum) over (
        PARTITION BY
            category
    )
from police_incident_reports;

SELECT
    category,
    date_part('year', date) as year,
    count(category) OVER (
        PARTITION BY
            date_part('year', date)
    ) as category_count
FROM police_incident_reports
GROUP BY
    category,
    year
ORDER BY year, category_count DESC;

select *, row_number() over () from police_incident_reports;

select category, pddistrict, date, row_number() over (
        partition by
            category
    )
from police_incident_reports

select category, pddistrict, date, row_number() over (
        PARTITION BY
            category
        order by date
    )
from police_incident_reports

select *
from (
        select
            category, pddistrict, date, row_number() over (
                PARTITION BY
                    category
                order by date
            ) as rn
        from police_incident_reports
    ) x
where
    x.rn <= 3;

select category, pddistrict, rank() over (
        PARTITION BY
            "Incident Code"
        order by pddistrict
    )
from police_incident_reports;

select category, pddistrict, dense_rank() over (
        PARTITION BY
            "Incident Code"
        order by pddistrict
    )
from police_incident_reports;