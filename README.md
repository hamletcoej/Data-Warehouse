# SPARKIFY

## Motivation

Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They have grown their user base and song database and want to move their processes and data onto the cloud.

## Tech used

Built with Amazon Web Services:
- S3 storage
- Redshift database


## Arcitechure

Sparkify data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

An ETL pipeline is required that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to

Tables required:
stagingevents - log data from s3://udacity-dend/log_data
stagingsongs - song data from s3://udacity-dend/song_data

Fact Table
songplays - records in event data associated with song plays i.e. records with page NextSong

Dimension Tables
users - users in the app
songs - songs in music database
artists - artists in music database
time - timestamps of records in songplays broken down into specific units



# Running the Process

To run it manually you can launch the create_tables.py and then etl.py process from the command line by typing 'python create_tables.py' then 'python etl.py'.
This process could be set up to execute on a task scheduler. 


## Script Files

- sql_queries.py
This script specifies what database tables we need dropped, created and inserted into.    

- create_tables.py
This script sets up the database connection and drops and created the tables specified sql_queries.py. 

- etl.py
This is the main script which copies the data from the S3, stages these into the redshift database and then inserts data from staging tables into fact and dimension tables.
