# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location TEXT,
    user_agent VARCHAR
);    
""")
#user_id, first_name, last_name, gender, level
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
);
""")
#song_id, title, artist_id, year, duration
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration INT
);
""")
#artist_id, name, location, latitude, longitude
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location TEXT,
    latitude FLOAT,
    longitude FLOAT
);
""")
#start_time, hour, day, week, month, year, weekday
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday VARCHAR NOT NULL
);
""")

# INSERT RECORDS

#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s
);
""")

#user_id, first_name, last_name, gender, level
user_table_insert = ("""
INSERT INTO users (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO NOTHING
;
""")

#song_id, title, artist_id, year, duration
song_table_insert = ("""
INSERT INTO songs (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING
;
""")

#artist_id, name, location, latitude, longitude
artist_table_insert = ("""
INSERT INTO artists (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING
;
""")

#start_time, hour, day, week, month, year, weekday
time_table_insert = ("""
INSERT INTO time (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING
;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id 
FROM songs s
JOIN artists a ON s.artist_id = a.artist_id
WHERE s.title = %s 
AND a.name = %s 
AND s.duration = %s
;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
insert_record_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]