-- DDL for actors table:

--CREATE TYPE films as(
--	film TEXT,
--	votes integer,
--	rating REAL,
--	filmid TEXT
--);

--CREATE TYPE quality_class AS ENUM ('star', 'good', 'average','bad');

--CREATE TABLE actors(
--	actor TEXT,
--	actor_id text,
--	current_year integer,
--	films films[],
--	quality_class quality_class,
--	is_active boolean,
--	PRIMARY KEY (actor_id,current_year,films)
--)


-- Cumulative table generation query: Write a query that populates the actors table one year at a time.
--INSERT INTO actors
WITH yesterday AS (
	SELECT * FROM actors   
	WHERE current_year = 1980  -- Started FROM 1969 AND kept incrementing. 1970 IS the FIRST DATA point IN actor_films
),
	today as(
	SELECT 
	max(actor) AS actor,
	actorid,
	array_agg(ROW(film,votes,rating,filmid)::films) AS film,
	YEAR,
	CASE 
		WHEN avg(rating) > 8 THEN 'star'
		WHEN avg(rating) > 7 THEN 'good'
		WHEN avg(rating) > 6 THEN 'average'
		ELSE 'bad'
	END::quality_class AS rating
	FROM actor_films
	WHERE year = 1981
	GROUP BY actorid, year
)
SELECT 
COALESCE (t.actor,y.actor) AS actor,
COALESCE (t.actorid,y.actor_id) AS actor_id,
COALESCE (t.year, y.current_year + 1) AS current_year,
CASE WHEN y.films IS NULL THEN t.film
	WHEN t.film IS NOT NULL THEN y.films || t.film
	ELSE y.films
END AS films,	
CASE 
	WHEN t.rating IS NOT NULL THEN t.rating
	ELSE y.quality_class 
END AS quality_class, 
CASE 
	WHEN t.year IS NOT NULL THEN true
	ELSE false
END AS is_active
FROM today t FULL OUTER JOIN yesterday y 
ON t.actorid = y.actor_id


-- DDL for actors_history_scd
--CREATE TABLE actors_history_scd(
--	actor TEXT,
--	actor_id TEXT,
--	quality_class quality_class,
--	is_active boolean,
--	current_year integer,
--	start_date integer,
--	end_date integer,
--	PRIMARY KEY(actor_id, start_date)
--);



-- Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
-- INSERT INTO actors_history_scd
WITH with_previous as( -- This aims TO ADD LAG TO see previous season DATA AND GET it TO the NEXT year. LIKE FFILL mainly FOR scoring AND isactive
	SELECT 
		actor,
		actor_id,
		quality_class,
		is_active,
		current_year,
	lag(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_quality_class,
	lag(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_is_active
	FROM actors
	WHERE current_year <= 1981 -- This IS ONLY added to add an incremental query below
	
),
with_indicators as( -- This one IS FOR adding flags TO see IF anything has changed
	SELECT *,
	CASE WHEN quality_class <> previous_quality_class THEN 1
		WHEN is_active <> previous_is_active THEN 1
	ELSE 0
	END AS change_indicator
	FROM with_previous
),
with_streaks as( --  Now we aim TO count the number OF changes LIKE a CHANGE streak.
	SELECT *,
		sum(change_indicator) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
	FROM with_indicators
)
SELECT 
	actor,
	actor_id,
	quality_class,
	is_active, 
	1981 AS current_year, -- hard coding 1979 because we want TO play WITH 1980(future) DATA below
	min(current_year) AS start_date,
	max(current_year) AS end_date
FROM with_streaks
GROUP BY actor,actor_id, quality_class,is_active, streak_identifier
order BY actor, streak_identifier



--- Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.

--CREATE TYPE scd_type2 as(
--	quality_class quality_class,
--	is_active boolean,
--	start_date integer,
--	end_date integer
--)

WITH last_year_scd as( -- Basically GET DATA OF LAST YEAR i.e. 1981.
	SELECT * FROM actors_history_scd
	WHERE current_year = 1981
	AND end_date = 1981
),
historical_scd AS( -- This IS DATA FROM BEFORE 1981
	SELECT
	actor,
	actor_id,
	quality_class,
	is_active,
	start_date,
	end_date
	FROM actors_history_scd
	WHERE current_year = 1981
	AND end_date < 1981
	
),
this_year_data AS ( -- This IS 1982 DATA.
	SELECT * FROM actors
	WHERE current_year = 1982
),
unchanged_records as(
	-- Records from the start of a actors journey till 1981 can be a single record
	SELECT ts.actor,
		ts.actor_id,
		ts.quality_class, 
		ts.is_active,
		ls.start_date,
		ls.current_year AS end_date
	FROM this_year_data ts
	JOIN last_year_scd ls 
	ON ls.actor_id = ts.actor_id
	WHERE ts.quality_class = ls.quality_class
	AND ts.is_active = ls.is_active
),
changed_records AS (
	-- If an actor is in 1982, we want an additional row with updated stats.
	SELECT ts.actor,ts.actor_id,
		
		UNNEST(array[
			row(
				ls.quality_class,
				ls.is_active, 
				ls.start_date,
				ls.end_date
			)::scd_type2,
			row(
				ts.quality_class,
				ts.is_active, 
				ts.current_year,
				ts.current_year
			)::scd_type2
		]) AS records
	FROM this_year_data ts
	LEFT JOIN last_year_scd ls 
	ON ls.actor_id = ts.actor_id
	WHERE (ts.quality_class <> ls.quality_class OR ts.is_active = ls.is_active)
	OR ls.actor_id IS null
),
unnested_changed_records AS ( -- We're just flattening the above RESULT here. Otherwise we have a SET which IS harder TO READ.
	SELECT actor,actor_id,
	(records::scd_type2).quality_class,
	(records::scd_type2).is_active,
	(records::scd_type2).start_date,
	(records::scd_type2).end_date
	FROM changed_records
	
),
new_records as(
	SELECT ts.actor,ts.actor_id,
	ts.quality_class,
	ts.is_active,
	ts.current_year AS start_date,
	ts.current_year AS end_date
	FROM this_year_data ts LEFT JOIN last_year_scd ls
	ON ts.actor_id = ls.actor_id
	WHERE ls.actor_id IS null
)
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL 
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records


