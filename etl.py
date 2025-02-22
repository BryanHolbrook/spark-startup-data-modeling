import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files


def process_song_file(cur, filepath):
    #song_files = get_files('data/song_data')
    #filepath = '/workspace/home/data/song_data/A/A/A/TRAAAMQ128F1460CD3.json'

    with open(filepath, 'r') as f:
        song_data = json.load(f)
    
    # open song file
    df = pd.DataFrame([song_data])

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year','duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name','artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    #log_files = get_files('data/log_data')
    #filepath = '/workspace/home/data/log_data/2018/11/2018-11-28-events.json'

    with open(filepath, 'r') as f:
        log_data = [json.loads(line) for line in f]
    
    # open log file
    df = pd.DataFrame(log_data)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'] / 1000, unit='s')
    
    # insert time data records
    time_data = [
    t,                 
    t.dt.hour,         
    t.dt.day,          
    t.dt.week, 
    t.dt.month,        
    t.dt.year,         
    t.dt.day_name()
]
    time_data = list(zip(*time_data))
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels).dropna()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].dropna() 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        start_time = pd.to_datetime(row.get('ts', 0), unit='ms')
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.get('song'), row.get('artist'), row.get('length')))
        results = cur.fetchone()
        
        if results and len(results) == 2:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None 
    
        # insert songplay record
        songplay_data = (
        start_time, row.get('userId'), row.get('level'),
        song_id, artist_id, row.get('sessionId'),
        row.get('location'), row.get('userAgent')
        )
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