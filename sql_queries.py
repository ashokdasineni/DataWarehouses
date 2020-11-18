import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIMES"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events(
 artist VARCHAR,
 auth VARCHAR,
 firstName VARCHAR,
 gender CHAR,
 itemInSession INTEGER,
 lastName VARCHAR,
 length VARCHAR,
 level VARCHAR,
 location VARCHAR,  
 method VARCHAR,
 page VARCHAR,
 registration DECIMAL,
 sessionid INTEGER,
 song VARCHAR,
 status INTEGER,
 ts BIGINT ,
 userAgent VARCHAR,
 userId INTEGER
 )
""")

staging_songs_table_create = ("""
create table if not exists staging_songs(
    song_id VARCHAR,
    num_songs INTEGER, 
    title VARCHAR, 
    artist_name VARCHAR,
    artist_latitude DECIMAL,
    year INTEGER,
    duration DECIMAL,
    artist_id VARCHAR,
    artist_longitude DECIMAL,
    artist_location VARCHAR
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id INTEGER IDENTITY(0,1) SORTKEY DISTKEY,
start_time TIMESTAMP NOT NULL,
user_id INTEGER NOT NULL,
level VARCHAR NOT NULL,
song_id VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
session_id INTEGER NOT NULL,
location VARCHAR NOT NULL,
user_agent VARCHAR NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE IF NOT exists USERS(
user_id INTEGER NOT NULL SORTKEY DISTKEY,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
gender VARCHAR,
level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS SONGS(
song_id VARCHAR NOT NULL SORTKEY DISTKEY,
title VARCHAR,
artist_id VARCHAR NOT NULL,
year INTEGER,
duration DECIMAL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS ARTISTS(
artist_id VARCHAR NOT NULL SORTKEY DISTKEY, 
name VARCHAR NOT NULL,
location VARCHAR,
lattitude DECIMAL,
longitude DECIMAL
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS TIMES(
start_time TIMESTAMP NOT NULL SORTKEY,
hour INTEGER NOT NULL,
day INTEGER NOT NULL,
week INTEGER NOT NULL,
month INTEGER NOT NULL,
year INTEGER NOT NULL,
weekday BOOLEAN NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = f"""copy staging_events from {config.get("S3","LOG_DATA")}
iam_role {config.get("IAM_ROLE","ARN")}
format as json {config.get("S3","LOG_JSONPATH")} region 'us-west-2'; """

staging_songs_copy = f"""copy staging_songs from {config.get("S3","SONG_DATA")}
iam_role {config.get("IAM_ROLE","ARN")}
format as json 'auto' region 'us-west-2'; """


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays ( 
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent) 
SELECT date_add('ms',SE.ts,'1970-01-01') as start_time, 
        SE.userId,
        SE.level,
        SS.song_id,
        SS.artist_id,
        SE.sessionid,
        SE.location,
        SE.userAgent
from staging_events SE 
     join 
     staging_songs SS
     on SE.song=SS.title and SE.length=SS.duration
where SE.page='NextSong'
""")

user_table_insert = ("""
insert into USERS(
       user_id,
       first_name,
       last_name,
       gender,
       level)
SELECT distinct SE.userId,
       SE.firstName,
       SE.lastName,
       SE.gender,
       SE.level 
from staging_events SE
WHERE SE.userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO SONGS(
        song_id,
        title,
        artist_id,
        year,
        duration
        )
select distinct SS.song_id,
       SS.title,
       SS.artist_id,
       SS.year,
       SS.duration 
from staging_songs SS
""")

artist_table_insert = ("""
INSERT INTO ARTISTS(
        artist_id, 
        name,
        location,
        lattitude,
        longitude
        )
SELECT distinct SS.artist_id,
        SS.artist_name,
        SS.artist_location,
        SS.artist_latitude,
        SS.artist_longitude 
from staging_songs SS      
""")

time_table_insert = ("""
INSERT INTO TIMES(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
    )
select 
    ts,
    date_part(h, ts) as hour,
    date_part(d, ts) as day,
    date_part(w, ts) as week,
    date_part(MON, ts) as month,
    date_part(y, ts) as year,
    case when date_part(dow, ts) <=5 then True Else False end as weekday 
from (select distinct date_add('ms',SE.ts,'1970-01-01') as ts 
      from staging_events SE 
      ) x
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

