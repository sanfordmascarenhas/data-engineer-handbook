/* 
 * This dataset contains player data and attributes together which is space heay.
 * 
 * We're trying to solve the compression issue. 
 * 
 * So we'll separate values that keep chaning to the ones that don't change.
 * 
 * Idea is that the ones that don't change can be compressed 
 * The shuffling problem of spark joins could be solved this way.
 * 
 * 
 * A properly modeled SCD table is in file 2.
 * I'm trying to learn here so not everything maybe correct/to my understanding.
 */

--select * from player_seasons;


-- We select the first 4 attributes that keep changing.
--create type season_stats as(
--							season INTEGER,
--							gp INTEGER,
--							pts REAL,     --I'm guessing this is like float.
--							reb REAL,
--							ast REAL
--)

-- CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average','bad'); -- To be used in player table

-- Now the player data which doesn't typically change and doesn't need to be duplicated unnecessarily.
--create table players(
--						player_name text,
--						height text,
--						college text,
--						country TEXT,
--						draft_year TEXT,
--						draft_round TEXT,
--						draft_number TEXT,
--						season_stats season_stats[],
--						scoring_class scoring_class,
--						years_since_last_season integer,
--						current_season integer,
--						is_active boolean,
--						PRIMARY KEY(player_name, current_season)
--)


-- INSERT INTO players  -- For safekeeping with the below 'WITH' clause
-- The first time you run this below stuff, it'll be null from players since  it's empty.
-- But now you will have a pipeline each time you run the insert query from above.
WITH yesterday AS (
	SELECT * FROM players   -- This was an EMPTY TABLE AT FIRST.
	WHERE current_season = 2021  -- Started FROM 1995 AND kept incrementing.
),
	today as(
	SELECT * FROM player_seasons
	WHERE season = 2022
	)
	
SELECT 
COALESCE (t.player_name,y.player_name) AS player_name,
COALESCE (t.height,y.height) AS height,
COALESCE (t.college,y.college) AS college,
COALESCE (t.country,y.country) AS country,
COALESCE (t.draft_year,y.draft_year) AS draft_year,
COALESCE (t.draft_round,y.draft_round) AS draft_round,
COALESCE (t.draft_number,y.draft_number) AS draft_number,
-- Now we're trying to get the season stats from yesterday or today into our working table.
-- If yesterday is null, pull from today. But if today is null then we do not want to wipe their data.
-- So if today not null, get that data otherwise just stick with yesterday's data.
-- Also, we concat to the array so it should keep getting bigger until there's no more today data. 
-- We avoid appending nulls from today's value as much as we can.
CASE WHEN y.season_stats IS NULL THEN array[ROW(t.season,t.gp,t.pts,t.reb,t.ast)::season_stats]
	WHEN t.season IS NOT NULL THEN y.season_stats || array[ROW(t.season, t.gp,t.pts,t.reb,t.ast)::season_stats]
	ELSE y.season_stats
	END AS season_stats,	
CASE WHEN t.season IS NOT NULL THEN -- This IS FOR the scoring ATTRIBUTE.
	CASE WHEN t.pts > 20 THEN 'star'
		WHEN t.pts > 15 THEN 'good'
		WHEN t.pts > 10 THEN 'average'
		ELSE 'bad'
	END::scoring_class
ELSE y.scoring_class -- IF there's NOTHING IN 'today' don't pull NULL. Just keep the OLD VALUES. it's an ENUM. strictly those vales.
END AS scoring_class, 
CASE WHEN t.season IS NOT NULL THEN 0 -- Just means that IF they aren't playing currently, INCREMENT the counter.
ELSE y.years_since_last_season + 1
END AS years_since_last_season,
COALESCE (t.season, y.current_season + 1) AS current_season, -- This IS TO GET how long ago was the previous season
CASE WHEN t.season IS NOT NULL THEN true
ELSE false
END AS is_active
FROM today t FULL OUTER JOIN yesterday y 
ON t.player_name = y.player_name

------------------------------------------------------------------------------------

-- You can see the table from here. 
SELECT * FROM players WHERE current_season = 2001
AND player_name = 'Michael Jordan';
-- This will give you one row with Michael and all his data.



-- Now this will break that grouped data into each row with unnested.
-- We put it in 'WITH' like a function so it's easier to write for outside.
WITH unnested AS (
	SELECT player_name, 
	UNNEST(season_stats):: season_stats AS season_stats
	FROM players
	WHERE current_season = 2001
)
SELECT player_name,
		(season_stats::season_stats),*	
FROM unnested
WHERE player_name = 'Michael Jordan' -- Add this to see a better version of the above query
-- The above achieves a sorted table which run time encoding can compress easily
-- This table can be joined further. Sort once only and no more needed philosophy.




---------------------------------

-- This is the final magic. No group by needed and sorted.
SELECT 
	player_name,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts/
	CASE WHEN (season_stats[1]::season_stats).pts = 0  -- TO prevent divide BY 0
	THEN 1 
	ELSE (season_stats[1]::season_stats).pts
	END AS improvent_times
FROM players
WHERE current_season = 2001

-- Incrementally builds up history
-- Access to historical analysis

