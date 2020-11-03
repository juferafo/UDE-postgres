from lib import get_columns, data_format

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(songplay_id int,
 ts bigint, 
 user_id int, 
 level varchar, 
 song_id varchar, 
 artist_id varchar, 
 session_id int, 
 location varchar, 
 user_agent varchar)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(user_id int,
 first_name varchar,
 last_name varchar,
 gender varchar,
 level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(song_id varchar PRIMARY KEY,
 title varchar,
 artist_id varchar,
 year int,
 duration float)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artist_id varchar PRIMARY KEY,
 artist_name varchar,
 artist_location varchar,
 artist_latitude int,
 artist_longitude int)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(start_time timestamp,
 hour int,
 day int,
 week int,
 month int,
 year int,
 weekday int)
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id, ts, user_id, level, song_id,
                           artist_id, session_id, location, user_agent)
    VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s);
""")

user_columns_str = get_columns(table="users")
insert_format = data_format(table="users")
user_table_insert = "INSERT INTO users {} VALUES {}".format(user_columns_str,\
                                                            insert_format)

song_columns_str = get_columns(table="songs")
insert_format = data_format(table="songs")
song_table_insert = "INSERT INTO songs {} VALUES {} ON CONFLICT DO NOTHING".format(song_columns_str,\
                                                                                   insert_format)

artist_columns_str = get_columns(table="artists")
insert_format = data_format(table="artists")
artist_table_insert = "INSERT INTO artists {} VALUES {} ON CONFLICT DO NOTHING".format(artist_columns_str,\
                                                                                       insert_format)

time_columns_str = get_columns(table="time")
insert_format = data_format(table="time")
time_table_insert = "INSERT INTO time {} VALUES {} ON CONFLICT DO NOTHING".format(time_columns_str,\
                                                                                  insert_format)

# FIND SONGS

song_select = "SELECT s.song_id, a.artist_id \
               FROM artists AS a \
               INNER JOIN songs AS s \
               ON a.artist_id = s.artist_id \
               WHERE s.title = %s AND \
                     a.artist_name = %s AND \
                     s.duration = %s"

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]