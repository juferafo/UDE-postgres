import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from lib import get_columns


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    # To avoid problems ingesting NaN values I convert these values into None
    df = df.where(pd.notnull(df), None)

    # insert song record
    song_columns = get_columns(table="songs", as_string=False)
    song_data = df[song_columns].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_columns = get_columns(table="artists", as_string=False)
    artist_data = df[artist_columns].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)
    # To avoid problems ingesting NaN values I convert these values into None
    df = df.where(pd.notnull(df), None)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms') 
    
    df["hour"] = t.dt.hour
    df["day"] = t.dt.day
    df["weekofyear"] = t.dt.weekofyear
    df["month"] = t.dt.month
    df["year"] = t.dt.year
    df["weekday"] = t.dt.weekday
    
    # insert time data records
    time_data = (t,\
                 t.dt.hour,\
                 t.dt.day,\
                 t.dt.weekofyear,\
                 t.dt.month,\
                 t.dt.year,\
                 t.dt.weekday) 
    
    column_labels = ("tsdt", "hour", "day", "weekofyear", "month", "year", "weekday")
    
    time_dic = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dic) 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_columns = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = df[user_columns]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)
            

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()