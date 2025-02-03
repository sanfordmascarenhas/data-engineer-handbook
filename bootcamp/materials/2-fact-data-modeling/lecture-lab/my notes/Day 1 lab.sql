--CREATE TABLE fct_game_details(
--	dim_game_date date,
--	dim_season integer,
--	dim_team_id integer,
--	dim_player_id integer,
--	dim_player_name TEXT,
--	dim_start_position TEXT,
--	dim_is_playing_at_home boolean,
--	dim_did_not_play boolean,
--	dim_did_not_dress boolean,
--	dim_not_with_team boolean,
--	m_minutes REAL,
--	m_fgm integer,
--	m_fga integer,
--	m_fg3m integer,
--	m_fg3a integer,
--	m_ftm integer,
--	m_oreb integer,
--	m_dreb integer,
--	m_reb integer,
--	m_ast integer,
--	m_stl integer,
--	m_blk integer,
--	m_turnovers integer,
--	m_pf integer,
--	m_pts integer,
--	m_plus_minus integer,
--	PRIMARY key(dim_game_date, dim_team_id, dim_player_id)	
--)


--INSERT INTO fct_game_details
--WITH deduped as( -- we dedupe the DATA since fact DATA can have duplicates. We ALSO ONLY MERGE what IS hard TO JOIN IN later. denormalizing basically.
--	SELECT 
--	g.game_date_est,
--	g.season,
--	g.home_team_id,
--	g.visitor_team_id,
--	gd.*,
--	row_number() over(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
--	FROM game_details gd 
--	JOIN games g ON gd.game_id = g.game_id
--)
--SELECT 
--	game_date_est AS dim_game_date,
--	season AS dim_season,
--	team_id AS dim_team_id,
--	player_id AS dim_player_id,
--	player_name AS dim_player_name,
--	start_position AS dim_start_position,
--	team_id = home_team_id AS dim_is_playing_at_home,
--	coalesce(POSITION('DNP' IN comment),0) > 0 AS dim_did_not_play,
--	coalesce(POSITION('DND' IN comment),0) > 0 AS dim_did_not_dress,
--	coalesce(POSITION('DNP' IN comment),0) > 0 AS dim_not_with_team,
--	CAST(split_part(min,':',1) AS REAL)+
--	CAST(split_part(min,':',2) AS real)/60 AS m_minutes,
--	fgm AS m_fgm,
--	fga AS m_fga,
--	fg3m AS m_fg3m,
--	fg3a AS m_fg3a,
--	ftm AS m_ftm,
--	fta AS m_fta,
--	oreb AS m_oreb,
--	reb AS m_reb,
--	ast AS m_ast,
--	stl AS m_stl,
--	blk AS m_blk,
--	"TO" AS m_turnovers,
--	pf AS m_pf,
--	pts AS m_pts,
--	plus_minus AS m_plus_minus
--FROM deduped
--WHERE row_num= 1


-- It's still easy to merge and get data from the tables we ignored.
-- while still keeping it small.


-- Demo query to see bailed % which is pretty fast.
SELECT 
count(1) AS num_games,
dim_player_name, 
count(CASE WHEN dim_not_with_team THEN 1 end) AS bailed_num,
cast(count(CASE WHEN dim_not_with_team THEN 1 end) AS real)/count(1) AS bail_pct
FROM fct_game_details
GROUP BY 2
ORDER BY 4 desc