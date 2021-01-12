import psycopg2
from lib import get_columns

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time BIGINT, 
        user_id INT, 
        level VARCHAR, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR,
        CONSTRAINT fk_user
            FOREIGN KEY(user_id) 
            REFERENCES users(user_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
        CONSTRAINT fk_song
            FOREIGN KEY(song_id) 
            REFERENCES songs(song_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,  
        CONSTRAINT fk_artists
            FOREIGN KEY(artist_id) 
            REFERENCES artists(artist_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE, 
        CONSTRAINT fk_time
            FOREIGN KEY(start_time) 
            REFERENCES time(start_time)
            ON DELETE CASCADE
            ON UPDATE CASCADE
        )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
        )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration FLOAT
        )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        artist_name VARCHAR,
        artist_location VARCHAR,
        artist_latitude INT,
        artist_longitude INT
        )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id,
                           artist_id, session_id, location, user_agent)
    VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) 
    DO UPDATE SET level = excluded.level
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = "SELECT s.song_id, a.artist_id \
               FROM songs AS s \
               JOIN artists AS a \
               ON a.artist_id = s.artist_id \
               WHERE s.title = %s AND \
                     a.artist_name = %s AND \
                     s.duration = %s"

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]