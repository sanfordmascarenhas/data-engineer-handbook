/**
 * We create vertices first for games, players and teams.
 */

-- This is a vertex of game
-- INSERT INTO vertices 
SELECT 
	game_id AS identifier,
	'game'::vertex_type AS TYPE,
	json_build_object(
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
	) AS properties
FROM games


-- Now a vertex of players
-- INSERT INTO vertices 
WITH players_agg AS( -- FIRST we GET just the DATA we want. Agg them BY player
	SELECT player_id AS identifier,
		max(player_name) AS player_name,
		count(1) AS number_of_games,
		sum(pts) AS total_points,
		array_agg(DISTINCT team_id) AS teams
	FROM game_details
	GROUP BY player_id
)
SELECT 
	identifier, 
	'player'::vertex_type,
	json_build_object(
		'player_name', player_name,
		'number_of_games', number_of_games,
		'total_points', total_points,
		'teams', teams
		)
FROM players_agg		

-- This is a vertex of teams
-- INSERT INTO vertices
WITH teams_deduped as( -- So the teams TABLE has duplicates. You can GROUP BY AND remove them but this IS cool TO DO AND cleaner.
	SELECT *, row_number() over(PARTITION BY team_id) AS row_num
	FROM teams
) -- Now we ONLY SELECT the FIRST COPY OF EACH PARTITION/duplicate TO INSERT INTO vertices.
SELECT 
	team_id AS identifier,
	'team'::vertex_type AS TYPE,
	json_build_object(
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena,
		'year_founded', yearfounded
	)
FROM teams_deduped
WHERE row_num = 1;

