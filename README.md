# DataLake with Spark and S3+EMR
##### [Israel Mendes](israelmendes.com.br)
Creating a data lake with Amazon S3 and EMR and Spark, running with PySpark.

--- 

## Process
Step By Step:
1. Fill the `dl.cfg` file with configuration data from the Data Lake in S3 of my account. For security reasons, the file was not included/or empty;
2. Run the `etl.py` script, to create and organize the following:
 - Select the sources and create the tables in PySpark;
 - Read the data from the sources and insert in the tables.
 - Create an output file to present the cleaned data.
 
## Command
The only command needed is:
> python etl.py

Remember to create and update `dl.cfg`.

## ETL
The main ideia is to extract data from a DataLake in S3, transform it and to load on AWS EMR. Here is the table and content for the data:

**Fact Table:**
- _songplays:_ songplays - records in log data associated with song plays i.e. records with page `NextSong`;
> songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent 

**Dimension Tables**
- _users:_ users in the app;
> user_id, first_name, last_name, gender, level
- _songs:_ songs in music database;
> user_id, first_name, last_name, gender, level
- _artists:_ artists in music database;
> user_id, first_name, last_name, gender, level
- _time:_ timestamps of records in songplays broken down into specific units;
> start_time, hour, day, week, month, year, weekday
