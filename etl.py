import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cursor, path):
    #Processes the song files and inserts data into db


    # open song file
    df = pd.DataFrame([pd.read_json(path, typ='series', convert_dates=False)])

    for value in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = value

        #insert artist record
        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cursor.execute(artist_table_insert, artist_data)

        #insert song record
        song_data = (song_id, title, artist_id, year, duration)
        cursor.execute(song_table_insert, song_data)
    
    print(f"Records inserted {path}")


def process_log_file(cursor, path):
    #Processes the event log files and insert data into db
 
    #opens the log file
    df = pd.read_json(path, lines=True)

    #filters by "page":"NextSong" action (rather than "Home")
    df = df[df['page'] == "NextSong"].astype({'ts': 'datetime64[ms]'})

    #converts timestamp column to datetime
    timestamp = pd.Series(df['ts'], index=df.index)
    
    #inserts time data records
    column_labels = ["timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"]
    time_data = []
    for data in timestamp:
        time_data.append([data, data.hour, data.day, data.weekofyear, data.month, data.year, data.day_name()])

    time_df = pd.DataFrame.from_records(data = time_data, columns = column_labels)

    for _, row in time_df.iterrows():
        cursor.execute(time_table_insert, list(row))


    #loads user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    #inserts user records
    for _, row in user_df.iterrows():
        cursor.execute(user_table_insert, row)

    #inserts songplay records
    for _, row in df.iterrows():
        
        #gets songid and artistid from song and artist tables
        cursor.execute(song_select, (row.song, row.artist, row.length))
        results = cursor.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        #inserts songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cursor.execute(songplay_table_insert, songplay_data)


def process_data(cursor, connection, path, func):
    #Driver function to load data from songs and event log files into Postgres database.
    #gets all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(path):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    #iterates over files and process
    for _, datafile in enumerate(all_files, 1):
        func(cursor, datafile)
        connection.commit()

def main():
    #loads song data and log data into the db
    connection = psycopg2.connect("host=127.0.0.1 dbname=songs_db user=postgres password=test")
    cursor = connection.cursor()

    process_data(cursor, connection, path='data/song_data', func=process_song_file)
    process_data(cursor, connection, path='data/log_data', func=process_log_file)

    connection.close()


if __name__ == "__main__":
    main()
    print("\n\nFinished processing!!!\n\n")