import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
     artist           VARCHAR     NULL,
     auth             VARCHAR     NULL,
     firstName        VARCHAR     NULL,
     gender           VARCHAR     NULL,
     itemInSession    VARCHAR     NULL,
     lastName         VARCHAR     NULL,
     length           VARCHAR     NULL,
     level            VARCHAR     NULL,
     location         VARCHAR     NULL,
     method           VARCHAR     NULL,
     page             VARCHAR     NULL,
     registration     VARCHAR     NULL,
     sessionId        INTEGER     NULL,
     song             VARCHAR     NULL,
     status           INTEGER     NULL,
     ts               INTEGER     NOT NULL,
     userAgent        VARCHAR     NULL,
     userId           INTEGER     NULL 
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
     num_songs         INTEGER     NULL,
     artist_id         VARCHAR     NOT NULL,
     artist_latitude   VARCHAR     NULL,
     artist_longitude  VARCHAR     NULL,
     artist_location   VARCHAR     NULL,
     artist_name       VARCHAR     NULL,
     song_id           VARCHAR     NOT NULL,
     title             VARCHAR     NULL,
     duration          DECIMAL     NULL,
     year              INTEGER     NULL
)

""")

# Star Schema tables:

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
     songplay_id     INTEGER IDENTITY(0,1)  NOT NULL, 
     start_time       TIMESTAMP      NOT NULL  SORTKEY, 
     user_id          VARCHAR        NOT NULL  DISTKEY, 
     level            VARCHAR        NOT NULL, 
     song_id          VARCHAR        NOT NULL, 
     artist_id        VARCHAR        NOT NULL, 
     session_id       VARCHAR        NOT NULL, 
     location         VARCHAR        NULL, 
     user_agent       VARCHAR        NULL,
     PRIMARY KEY(songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
     user_id     INT           NOT NULL   SORTKEY,
     first_name  VARCHAR       NULL,
     last_name   VARCHAR       NULL,
     gender      VARCHAR       NULL,
     level       VARCHAR       NULL,
     PRIMARY KEY(user_id)
) 
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
     song_id     VARCHAR     NOT NULL SORTKEY,
     title       VARCHAR     NOT NULL,
     artist_id   VARCHAR     NOT NULL,
     year        INT         NOT NULL,
     duration    DECIMAL     NOT NULL,
     PRIMARY KEY(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
     artist_id   VARCHAR  NOT NULL   SORTKEY,
     name        VARCHAR  NULL,
     location    VARCHAR  NULL,
     latitude    DECIMAL  NULL,
     longitude   DECIMAL  NULL,
     PRIMARY KEY(artist_id)
) 
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
     start_time  TIMESTAMP    NOT NULL  SORTKEY,
     hour        INT          NULL,
     day         INT          NULL,
     week        INT          NULL,
     month       INT          NULL,
     year        INT          NULL,
     weekday     INT          NULL,
     PRIMARY KEY (start_time)
)
""")

# STAGING TABLES (copy data into staging tables)

staging_events_copy = ("""
     COPY staging_events FROM {}
     iam_role '{}'
     format as json {}
     region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role '{}'
    format as json 'auto'
    region 'us-west-2'
""").format(SONG_DATA,ARN)

# FINAL TABLES (copy data into star schema)

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
    SELECT DISTINCT se.ts        AS start_time,
        se.userId       AS user_id,
        se.level        AS level,
        ss.song_id      AS song_id,
        ss.artist_id    AS artist_id,
        se.sessionId    AS session_id,
        se.location     AS location,
        se.userAgent    AS user_agent
    FROM staging_events    AS se
    JOIN staging_songs     AS ss
       ON (se.artist = ss.artist_name)
       WHERE se.page = 'NextSong';
    
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level)
    SELECT DISTINCT se.userId   AS user_id,
        se.firstName            AS first_name,
        se.lastName             AS last_name,
        se.gender               AS gender,
        se.level                AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
         
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration)
    SELECT DISTINCT ss.song_id  AS song_id,
        ss.title                AS title,
        ss.artist_id            AS artist_id,
        ss.year                 AS year,
        ss.duration             AS duration
    FROM staging_songs AS ss;
    
""")

artist_table_insert = ("""
    INSERT INTO artists (
         artist_id,
         name,
         location,
         latitude,
         longitude)
    SELECT DISTINCT ss.artist_id   AS artist_id,
           ss.artist_name          AS name,
           ss.artist_location      AS location,
           ss.artist_latitude      AS latitude,
           ss.artist_longitude     AS longitude
    FROM staging_songs   AS ss;
""")

time_table_insert = ("""
    INSERT INTO time (
         start_time,
         hour,
         day,
         week,
         month,
         year,
         weekday)
    SELECT DISTINCT se.ts   AS start_time,
    EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events                   AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
