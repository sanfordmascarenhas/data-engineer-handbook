/*
 * This builds on the previous lab.
 * You need the players data upto 2022 in 'today' filled
 * We will then create a new table called players_scd
 * 
 * And we will create multiple partitions of player data 
 * Add flags and indicators
 * Collapse some as we see fit (?)
 * 
 * 
 * Idea is we have one table with user A for e.g. who has
 * a row for past data till yesterday
 * a row for today
 * 
 * and for user B for e.g.
 * a row for past data till last data point (login)
 * no new row or changes since you don't wanna pollute the data with null or anything.
 * 
 * Also not too aggregated like a nested set which is complex to query and analyse.
 *
 */

--CREATE TABLE players_scd(
--	player_name TEXT,
--	scoring_class scoring_class,
--	is_active boolean,
--	current_season integer,
--	start_season integer,
--	end_season integer,
--	PRIMARY KEY(player_name, start_season)
--);

-- INSERT INTO players_scd
WITH with_previous as( -- This aims TO ADD LAG TO see previous season DATA AND GET it TO the NEXT season. LIKE FFILL
	SELECT player_name,
	current_season,
	scoring_class,
	is_active,
	lag(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
	lag(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
	FROM players
	WHERE current_season <= 2021 -- This IS ONLY added so that we can make incremental builds AND experiment below.
	
),
with_indicators as( -- This one IS FOR adding flags TO see IF anything has changed
	SELECT *,
	CASE WHEN scoring_class <> previous_scoring_class THEN 1
		WHEN is_active <> previous_is_active THEN 1
	ELSE 0
	END AS change_indicator
	FROM with_previous
),
with_streaks as( --  Now we aim TO count the number OF changes LIKE a CHANGE streak.
	SELECT *,
		sum(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
	FROM with_indicators
)
SELECT player_name,
	scoring_class,
	is_active, 
	2021 AS current_season, -- hard coding 2021 because we want TO play WITH 2022(future) DATA IN players below
	min(current_season) AS start_season,
	max(current_season) AS end_season
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
order BY player_name, streak_identifier
 

SELECT * FROM players_scd

-- This is nice that we have all the data first and then we slice it.
-- But it's expensive at scale. Like Facebook scale not Airbnb scale
-- It scans all of history every time.
-- Cardinality goes if there's any small change which is insifigicant.


------------------------------------------------------------------

--CREATE TYPE scd_type as(
--	scoring_class scoring_class,
--	is_active boolean,
--	start_season integer,
--	end_season integer
--)

-- For this, think you're working in 2022 and now all the years make sense.
WITH last_season_scd as( -- Basically GET DATA OF LAST YEAR i.e. 2021.
	SELECT * FROM players_scd
	WHERE current_season = 2021
	AND end_season = 2021
),
historical_scd AS( -- This IS DATA FROM BEFORE 2021
	SELECT
	player_name, 
	scoring_class,
	is_active,
	start_season,
	end_season
	FROM players_scd
	WHERE current_season = 2021
	AND end_season < 2021
	
),
this_season_data AS ( -- This IS 2022 DATA.
	SELECT * FROM players
	WHERE current_season = 2022
),
unchanged_records as(
	-- Records from the start of a players journey till 2021 can be a single record
	SELECT ts.player_name,
		ts.scoring_class, 
		ts.is_active,
		ls.start_season,
		ls.current_season AS end_season
	FROM this_season_data ts
	JOIN last_season_scd ls 
	ON ls.player_name = ts.player_name
	WHERE ts.scoring_class = ls.scoring_class
	AND ts.is_active = ls.is_active
),
changed_records AS (
	-- If a player is in 2022, we want an additional row with updated stats.
	SELECT ts.player_name,
		
		UNNEST(array[
			row(
				ls.scoring_class,
				ls.is_active, 
				ls.start_season,
				ls.end_season
			)::scd_type,
			row(
				ts.scoring_class,
				ts.is_active, 
				ts.current_season,
				ts.current_season
			)::scd_type
		]) AS records
	FROM this_season_data ts
	LEFT JOIN last_season_scd ls 
	ON ls.player_name = ts.player_name
	WHERE (ts.scoring_class <> ls.scoring_class OR ts.is_active = ls.is_active)
	OR ls.player_name IS null
),
unnested_changed_records AS ( -- We're just flattening the above RESULT here. Otherwise we have a SET which IS harder TO READ.
	SELECT player_name,
	(records::scd_type).scoring_class,
	(records::scd_type).is_active,
	(records::scd_type).start_season,
	(records::scd_type).end_season
	FROM changed_records
	
),
new_records as(
	SELECT ts.player_name,
	ts.scoring_class,
	ts.is_active,
	ts.current_season AS start_season,
	ts.current_season AS end_season
	FROM this_season_data ts LEFT JOIN last_season_scd ls
	ON ts.player_name = ls.player_name
	WHERE ls.player_name IS null
)


SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL 
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records

-- The above large query makes the assumption that scoring_class and is_active isn't null otherwise in the comparison 
-- We could filter out data. Could use a function like is_distinct_from. Look it up.

-- This query is super quick but take care of assumptions and 
-- because of historical data, harder to backfill.