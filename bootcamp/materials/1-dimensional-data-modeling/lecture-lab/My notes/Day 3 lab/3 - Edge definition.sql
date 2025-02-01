

-- This is player and game edge
-- INSERT INTO edges
WITH deduped as( -- Bad DATA IMPORT resulted IN duplicated DATA. So we're numbering them here AND THEN filtering them outside.
	SELECT *, row_number() OVER (PARTITION BY player_id, game_id) AS row_num 
	FROM game_details
)
SELECT 
	player_id AS subject_identifier,
	'player'::vertex_type AS subject_type,
	game_id AS object_identifier,
	'game'::vertex_type AS object_type,
	'plays_in'::edge_type AS edge_type,
	json_build_object(
		'start_position', start_position,
		'pts', pts,
		'team_id', team_id,
		'team_abbreviation', team_abbreviation
	) AS properties
FROM deduped
WHERE row_num = 1

-- Demo query : What's the most points each player got in a game 
-- We just goup by player name and get the information stored in the edge i.e. points.
SELECT 
	v.properties->>'player_name',
	max(CAST(e.properties->>'pts' AS integer))
FROM vertices v JOIN edges e 
ON e.subject_identifier = v.identifier 
AND e.subject_type = v.TYPE 
GROUP BY 1
ORDER BY 2 DESC

-- This is player and player edge
-- Like who plays with/shares team and plays against whom.
-- INSERT INTO edges
WITH deduped as( -- Bad DATA IMPORT resulted IN duplicated DATA. So we're numbering them here AND THEN filtering them outside.
	SELECT *, row_number() OVER (PARTITION BY player_id, game_id) AS row_num 
	FROM game_details
),
filtered as( -- QUALIFY doesn't exist IN postgres LIKE IN snowflake. So we're filtering WITH this rather than the keyword.
	SELECT * FROM deduped WHERE row_num = 1
), 
aggregated AS (
	SELECT 
		f1.player_id AS subject_player_id,
		f2.player_id AS object_player_id,
		CASE 
			WHEN f1.team_abbreviation = f2.team_abbreviation 
			THEN 'shares_team'::edge_type
			ELSE 'plays_against'::edge_type
		END AS edge_type,
		max(f1.player_name) AS subject_player_name,
		max(f2.player_name) AS object_player_name,
		count(1) AS num_games,
		sum(f1.pts) AS subject_points,
		sum(f2.pts) AS object_points
	FROM
		filtered f1
	JOIN 
		filtered f2
	ON
		f1.game_id = f2.game_id
	AND
		f1.player_name <> f2.player_name
	WHERE 
		f1.player_name > f2.player_name -- This IS because we will GET 2 SETS OF inputs FROM the INNER JOIN.
	GROUP BY 1,2,3 -- don't DO this IS production. I'm just keeping it clean rn.
)
SELECT 
	subject_player_id AS subject_identifier,
	'player'::vertex_type AS subject_type,
	object_player_id AS object_identifier,
	'player'::vertex_type AS object_type,
	edge_type AS edge_type,
	json_build_object(
		'num_games', num_games,
		'subject_points', subject_points,
		'object_points', object_points
	) 
FROM 
	aggregated
	
	
--- Demo query : how does each player play against
	
SELECT
	v.properties->>'player_name' AS main_player,
	e.object_identifier AS playing_aginst_this_persons_id,
	cast(v.properties->>'number_of_games' AS real)/
	CASE 
		WHEN cast(v.properties->>'total_points' AS real) = 0
		THEN 1
		ELSE cast(v.properties->>'total_points' AS real)
	END AS average_points_per_game,
	e.properties->>'subject_points' AS player_points,
	e.properties->>'num_games' AS number_of_games
FROM
	vertices v
JOIN 
	edges e
ON 
	v.identifier = e.subject_identifier
AND 
	v.TYPE =e.subject_type
WHERE 
	e.object_type = 'player'::vertex_type
	
