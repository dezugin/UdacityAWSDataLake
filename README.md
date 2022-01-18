# Project: Udacity Data Lake
## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Schema for Song Play Analysis

Using the song and log datasets, i've created a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table

- songplays - records in log data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

- users - users in the app
    user_id, first_name, last_name, gender, level
- songs - songs in music database
    song_id, title, artist_id, year, duration
- artists - artists in music database
    artist_id, name, location, lattitude, longitude
- time - timestamps of records in songplays broken down into specific units
    start_time, hour, day, week, month, year, weekday

## Files in this project

- etl.py: reads data from S3, processes that data using Spark, and writes them back to S3
- dl.cfg: contains your AWS credentials
- README.md: provides discussion on your process and decisions
- etl_test_notebook.ipynb: jupyter notebook used to create and test etl process
    
## What I actually did

Created an ELT process to load data from either a local file (significantly faster) or a remote s3 bucket provided by udacity, transform the data in a star schema and load the data in a parquet format into either a local folder or a bucket created by an account provided by udacity as well.