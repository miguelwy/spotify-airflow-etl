import pandas as pd
import requests as rq
import sys
import json
import datetime
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append('/opt/airflow/dags/spotify-etl')
from config import config


TOKEN = 'BQATPzmpdIY2aeVTOetd2afWUGWUpNXH2nuAUZtbXGcTD1LOZFyieU9-PfqvIMy74A3cfyacyPSzX4zBrZ2kpSLE5-4nZTohDmLF442ZHoYqJAxEjYXh6iSlLVNUx-nOXxyOrHi_wVdOJs8A1PXipA'

def get_recently_played_songs():
    headers = {
        "Accept" : "application/json",
        "Content-Type":"application/json",
        "Authorization":"Bearer {token}".format(token=TOKEN)
    }

    try:
        today =  datetime.datetime.now()
        yesterday = today -datetime.timedelta(days=1)
        yesterday_unix_timestamp = int(yesterday.timestamp())
        r = rq.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix_timestamp),headers=headers)
        data = r.json()
        song_names = []
        artist_names = []
        played_at_list =[]
        timestamps = []

        for song in data['items']:
            song_names.append(song['track']['name'])
            artist_names.append(song['track']['album']['artists'][0]['name'])
            played_at_list.append(song['played_at'])
            timestamps.append(song['played_at'][0:10])
        
        song_dict = {
            "song_name" : song_names,
            "artist_name" : artist_names,
            "played_at": played_at_list,
            "timestamp" : timestamps
        }

        song_df = pd.DataFrame(song_dict, columns=['song_name','artist_name','played_at','timestamp'])
    except(Exception) as error:
        raise Exception("Error consulting spotify API: {error}".format(error=error))

    song_df.to_csv('song_df.csv',index=False)

def check_valid_df(df:pd.DataFrame) -> bool:
    if df.empty:
        print('No songs downloaded. Finishing execution.')
        return False     

    if pd.Series(df['played_at']).is_unique():
        pass
    else:
        raise Exception("Primary key check is violated")
        
    if df.isnull().values.any():
        raise Exception("Null value found")
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour= 0,minute = 0, second = 0,microsecond= 0)
    timestamps = df["timestamps"].to_list()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the songs do not come from the last 24 hours")
    return True

def check_postgresql_connection() -> bool:
    try:
        params = config()
        print("Connecting to database...")
        conn = psycopg2.connect(host=params['host'],database=params['database'],user=params['user'],password=params['password'],port=params['port'])
        cur = conn.cursor()
        print('PostgreSQL database version: ')
        cur.execute('SELECT version()')
        db_version= cur.fetchone()
        print(db_version)
        cur.close()
    except(Exception, psycopg2.DatabaseError) as error:
        raise Exception(("Error connecting to database: {error}".format(error=error)))
    finally:
        if conn is not None:
            conn.close()
    return True

def get_connection():
    """Connect to database and return connection object"""
    try:
        params = config()
        conn = psycopg2.connect(host=params['host'],database=params['database'],user=params['user'],password=params['password'],port=params['port'])
        if conn is not None:
            return conn 
    except(Exception,psycopg2.DatabaseError) as error:
        raise Exception("Error connecting to database: {error}".format(error=error))
    return conn

def create_table_my_played()-> bool:
    sql = """CREATE TABLE IF NOT EXISTS my_played_tracks(
                song_name VARCHAR(200),
                artist_name VARCHAR(200),
                played_at VARCHAR(200),
                timestamp VARCHAR(200),
                CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
            )
        """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        print("Created table my_played_at")
        cur.close()
    except:
        conn.rollback()
        raise Exception("Error creating my_played_at table")
    finally:
        if conn is not None:
            conn.close()
            

def insert_into_database() -> bool:
    """Copy data to memory then insert into database"""
    song_df = pd.read_csv('song_df.csv', index_col= None)
    conn = get_connection()
    try:
        print("Started insertion...")
        tuples = [tuple(x) for x in song_df.to_numpy()]
        cols = ','.join(list(song_df.columns))

        
        cur = conn.cursor()

        values = [cur.mogrify("(%s,%s,%s,%s)", tup).decode('utf-8') for tup in tuples]
        query  = "INSERT INTO %s(%s) VALUES " % ("my_played_tracks", cols) + ",".join(values)

        cur.execute(query,tuples)
        conn.commit()
        cur.close()
        print("Insertion finished!")
    except(Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        raise Exception("Error: {error}".format(error=error))
    finally:
        if conn is not None:
            conn.close()

# Register DAG in Apache Airflow
with DAG("spotify-etl", start_date=datetime.datetime(2021,1,1), schedule_interval="@hourly", catchup=False) as dag:
    get_recently_played_songs_data=PythonOperator(
        task_id="get_recently_played_songs_data",
        python_callable=get_recently_played_songs
    )

    insert_into_db=PythonOperator(
        task_id="insert_into_database",
        python_callable=insert_into_database
    )

    get_recently_played_songs_data  >> insert_into_db