
-- Creating this enum ensures that we create vertices of these types only.
CREATE TYPE vertex_type AS ENUM ('player','team', 'game'); -- Player who plays here IN this game 

CREATE TABLE vertices(
	identifier TEXT,
	TYPE vertex_type,
	properties json,
	PRIMARY KEY (identifier, type)
);





CREATE TYPE edge_type AS enum( 
	'plays_against', -- Main Relationship 
	'shares_team', -- This IS because You might have players playing against their own team LIKE a team b?
	'plays_in', -- plays IN game 
	'plays_on'  -- plays ON team
)

CREATE TABLE edges(
	subject_identifier TEXT,
	subject_type vertex_type, -- NOTICE how this ENUM IS used here AND even FOR OBJECT TYPE. 
	object_identifier TEXT,
	object_type vertex_type,
	edge_type edge_type,
	properties JSON,
	PRIMARY KEY(
		subject_identifier,
		subject_type,
		object_identifier,
		object_type,
		edge_type
	)
)
