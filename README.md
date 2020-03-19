# Sparkify's Relational Data Model with PostgreSQL

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new 
music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. 
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity 
on the app, as well as a directory with JSON metadata on the songs in their app.

They are in need of a PostgreSQL database with tables designed to optimize queries on song play analysis. The goal is to 
create a database schema and ETL pipeline for this analysis. 

## Schema for Song Analysis

The schema impleented follows a Star pattern, with four dimensional tables and a single fact tables, connected to all 
of them. Here's the data model in detail:

### Fact table

`songplays`: Stored log data associated with song plays.

```
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id SERIAL PRIMARY KEY,
    start_time BIGINT REFERENCES time(start_time),
    user_id VARCHAR(100) REFERENCES users(user_id),
    level VARCHAR(50),
    song_id VARCHAR(100) REFERENCES songs(song_id),
    artist_id VARCHAR(100) REFERENCES artists(artist_id),
    session_id INT,
    location VARCHAR(200),
    user_agent VARCHAR(200)
)
```

### Dimension tables

`users`: App users

```
CREATE TABLE IF NOT EXISTS users(
    user_id VARCHAR(100) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    gender CHAR(1) NOT NULL,
    level VARCHAR(50) NOT NULL
)
```

`songs`: Songs in the app's music database

```
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(100) PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    artist_id VARCHAR(100) NOT NULL,
    year INT NOT NULL,
    duration FLOAT NOT NULL
)
```

`artists`: Artists in the app's music database

```
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    location VARCHAR(200) NOT NULL,
    latitude FLOAT,
    longitude FLOAT
)
```

`time`: timestamps of records in `songplays`, broken down to their specific components

```
CREATE TABLE IF NOT EXISTS time(
    start_time BIGINT PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
)
```

## File Structure:

* `sql_queries.py`: Contains the definition of the creation statements for the tables, as well as the drop statements. It also defines the insert queries and, finally, the song selection query used to match `songplays` with `artists` and `songs`.
* `create_table.py`: Takes the DROP and CREATE statements defined in `sql_queries.py` to create a fresh instance of the `sparkify` database each time it's run.
* `etl.py`: Parses the song and log data, in JSON format, located at `data/`, and populates the database with it.

## Instructions

Make sure you have a PostgreSQL instance running and listening at `localhost:5432`. Also, ensure the configured user and password are `student`. Finally, the default db must be `studentdb`.

To create the data model, run: `python create_tables.py`

Finally, to populate the database, run: `python etl.py`