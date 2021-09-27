import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagingevents"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingsongs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stagingevents
                            (
                             artist varchar,
                             auth varchar,
                             firstName varchar,
                             gender varchar,
                             itemInSession int,
                             lastName varchar,
                             length numeric,
                             level varchar,
                             location varchar,
                             method varchar,
                             page varchar,
                             registration varchar,
                             sessionId int,
                             song varchar,
                             status int,
                             ts numeric,
                             userAgent varchar,
                             userId int 
                             );
                        """)




staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stagingsongs
                            (num_songs int,
                             artist_id varchar,
                             artist_latitude numeric,
                             artist_longitude numeric,
                             artist_location varchar, 
                             artist_name varchar, 
                             song_id varchar, 
                             title varchar, 
                             duration numeric,
                             year int
                                );
                        """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                            (
                                songplay_id int identity(0,1) PRIMARY KEY,
                                start_time timestamp NOT NULL, 
                                user_id int NOT NULL, 
                                level varchar NOT NULL, 
                                song_id varchar NOT NULL, 
                                artist_id varchar NOT NULL,
                                session_id int NOT NULL, 
                                location varchar NULL, 
                                user_agent varchar NULL
                            );
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                        (
                            user_id int PRIMARY KEY NOT NULL, 
                            first_name varchar NULL, 
                            last_name varchar NULL, 
                            gender varchar NULL, 
                            level varchar NOT NULL
                        );
                        """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
                        (
                            song_id varchar PRIMARY KEY NOT NULL, 
                            title varchar NOT NULL, 
                            artist_id varchar NOT NULL, 
                            year int, 
                            duration numeric NOT NULL
                        );
                        """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                          (
                              artist_id varchar PRIMARY KEY NOT NULL, 
                              artist_name varchar NOT NULL, 
                              artist_location varchar NULL, 
                              artist_latitude numeric NULL,
                              artist_longitude numeric NULL
                          );
                            """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                        (
                            start_time timestamp PRIMARY KEY NOT NULL, 
                            hour int NOT NULL,
                            day int NOT NULL, 
                            week_of_year int NOT NULL, 
                            month int NOT NULL, 
                            year int NOT NULL, 
                            weekday int NOT NULL
                        );
                        """)

# STAGING TABLES
staging_events_copy = (""" COPY stagingevents 
                           FROM {} credentials
                           'aws_iam_role={}'
                           region 'us-west-2' 
                           COMPUPDATE OFF STATUPDATE OFF
                           JSON {}
                        """).format(config.get('S3','LOG_DATA'),
                                    config.get('IAM_ROLE', 'ARN'),
                                    config.get('S3','LOG_JSONPATH'))


staging_songs_copy = (""" COPY stagingsongs
                            FROM {} credentials
                            'aws_iam_role={}' 
                            JSON 'auto' 
                            COMPUPDATE OFF region 'us-west-2'
                            """).format(config.get('S3','SONG_DATA'),
                                        config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (
                            user_id, 
                            level,
                            start_time,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent
                            )
                            SELECT
                                userId as user_id, 
                                level,
                                TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
                                song_id,
                                ss.artist_id,
                                sessionId as session_id,
                                location,
                                userAgent as user_agent
                            FROM stagingevents AS se
                            JOIN stagingsongs AS ss
                                ON se.artist = ss.artist_name
                            WHERE se.page = 'NextSong';
                                """)

user_table_insert = ("""INSERT INTO users 
                        (
                            user_id, 
                            first_name, 
                            last_name, 
                            gender, 
                            level
                        )
                        SELECT DISTINCT
                            userId AS user_id,
                            firstName,
                            lastName,
                            gender,
                            level
                            FROM stagingevents AS es1
                                WHERE userId IS NOT null
                                AND ts = (SELECT max(ts) 
                                          FROM stagingevents AS es2 
                                          WHERE es1.userId = es2.userId)
                        ORDER BY userId DESC;
                        """)



song_table_insert = ("""INSERT INTO songs 
                        (
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration
                        )
                        SELECT
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration
                        FROM stagingsongs;
                        """)

artist_table_insert = ("""INSERT INTO artists 
                         (
                             artist_id, 
                             artist_name, 
                             artist_location, 
                             artist_latitude, 
                             artist_longitude
                         )
                        SELECT
                            artist_id,
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude
                        FROM stagingsongs;
                        """)

time_table_insert = (""" INSERT INTO time 
                        (
                            start_time, 
                            hour, 
                            day, 
                            week_of_year, 
                            month, 
                            year, 
                            weekday
                        )
                        SELECT  
                            DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
                            EXTRACT(hour FROM start_time) AS hour,
                            EXTRACT(day FROM start_time) AS day,
                            EXTRACT(week FROM start_time) AS week,
                            EXTRACT(month FROM start_time) AS month,
                            EXTRACT(year FROM start_time) AS year,
                            EXTRACT(week FROM start_time) AS weekday

                        FROM stagingevents
                        WHERE stagingevents.page = 'NextSong';
                         """)


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
