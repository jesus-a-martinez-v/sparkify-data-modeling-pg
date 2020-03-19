import glob
import os

import pandas as pd
import psycopg2

from sql_queries import *


def process_song_file(cursor, filepath):
    """
    Processes the data in a single song file.
    :param cursor: Cursor used to execute statements in Postgres.
    :param filepath: Path to the song file to be processed.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df[artist_columns].values[0].tolist()
    cursor.execute(artist_table_insert, artist_data)

    # insert song record
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[song_columns].values[0].tolist()
    cursor.execute(song_table_insert, song_data)


def process_log_file(cursor, filepath):
    """
    Processes the data in a single log file.
    :param cursor: Cursor used to execute statements in Postgres.
    :param filepath: Path to the log file to be processed.
    """

    def get_timestamp_data(df):
        # convert timestamp column to datetime
        timestamp = pd.to_datetime(df['ts'], unit='ms')

        return (df['ts'].values,
                timestamp.dt.hour.values,
                timestamp.dt.day.values,
                timestamp.dt.week.values,
                timestamp.dt.month.values,
                timestamp.dt.year.values,
                timestamp.dt.weekday.values)

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # insert time data records
    time_data = get_timestamp_data(df)
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(data=dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cursor.execute(time_table_insert, list(row))

    # load user table
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_columns]

    # insert user records
    for i, row in user_df.iterrows():
        cursor.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get song_id and artist_id from song and artist tables
        cursor.execute(song_select, (row.song, row.artist, row.length))
        results = cursor.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = (
            row['ts'], row['userId'], row['level'], song_id, artist_id, row['sessionId'], row['location'],
            row['userAgent'])
        cursor.execute(songplay_table_insert, songplay_data)


def process_data(cursor, connection, filepath, function):
    """
    Processes the data, stored in JSON format, in a given directory, saving it in the corresponding data model in Postgres.
    :param cursor: Cursor used to interact with the database.
    :param connection: Connection to the database.
    :param filepath: Path to the directory with the JSONs to be processed.
    :param function: Function to process each JSON file located inside `filepath`
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        function(cursor, datafile)
        connection.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Populates the database with the song and log data located at data/song_data and data/log_data, respectively.
    """
    conn = psycopg2.connect('host=127.0.0.1 dbname=sparkifydb user=student password=student')
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', function=process_song_file)
    process_data(cur, conn, filepath='data/log_data', function=process_log_file)

    conn.close()


if __name__ == '__main__':
    main()
