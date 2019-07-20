# Using this ETL script to pull the data from a S3, transform it and output it another bucket. Using PySpark

import os
import configparser
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


## CONFIG.
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CONFIG']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CONFIG']['AWS_SECRET_ACCESS_KEY']


## SESSION WITH SPARK
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print(" ====> Creating the Spark Session.")
    return spark


## EXTRACTING AND LOADING DATA FROM SONG_TABLES
def process_song_data(spark, input_data, output_data):
    """
    The function to process Song Data.
    
    Parameters:
        spark: spark session to execute the code.
        input_data: The file path to extract the data from.
        output_data: The file path to load the data for.
    """
   
    # get filepath to song data file
    song_data = input_data['song_data']
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    print(' ====> Created the Song Table of Song_Data.')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "/songs/songs_table.parquet")
    print(" ====> Divided the data from Songs_Table.")
    
    # extract columns to create artists table
    artists_table = df.selectExpr(
        'artist_id', 
        'artist_name as name', 
        'artist_location as location', 
        'artist_latitude as latitude', 
        'artist_longitude as longitude'
    ).distinct()
    print(' ====> Created the Artists Table of Song_Data.')

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "/artists/artists_table.parquet")
    print(" ====> Divided the data from Artists_Table.")

    
## EXTRACTING AND LOADING DATA FROM LOG_TABLES
def process_log_data(spark, input_data, output_data):
    """
    The function to process Log Data. Very similar to the previous function.
    
    Parameters:
        spark: spark session to execute the code.
        input_data: The file path to extract the data from.
        output_data: The file path to load the data for.
    """
        
    # get filepath to log data file
    log_data = input_data['log_data']

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.[df.page == 'NextSong']

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT 
            userId AS user_id,
            firstName as first_name,
            lastName as last_name,
            gender,
            level
        FROM logs
    """).distinct()
    print(" ====> Created the Users Table.")
    
    # write users table to parquet files
    users.write.parquet(output_data + "/users/users_table.parquet")
    print(" ====> Divided the data from Users Table.")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x / 1000.0),IntegerType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: int(x / 1000.0),IntegerType())
    df = df.withColumn('start_time', from_unixtime(df.timestamp))
    
    # extract columns to create time table
    times = spark.sql("""SELECT ts_date FROM logs""").distinct()
    time_table = time_table.select(
        "ts_date".alias("start_time"),
        hour("ts_date").alias("hour"),
        dayofmonth("ts_date").alias("day"),
        weekofyear("ts_date").alias("week"),
        month("ts_date").alias('month'),
        year("ts_date").alias('year'),
        date_format("ts_date","EEEE").alias("weekday")
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "/time_table/time_table.parquet")
    print(" ====> Loaded the data into the Time Table of Log_Data.")
    
    # read in song data to use for songplays table
    song_df = input_data['song_data'].distinct()

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT distinct e.ts as start_time,
            e.userId as user_id,
            e.level as level,
            s.song_id as song_id,
            s.artist_id as artist_id,
            e.sessionId as session_id,
            e.location as location,
            e.userAgent as user_agent,
            year(date_time) as year,
            month(date_time) as month
        FROM logs_staging e join songs_staging s
            ON (e.song = s.title and s.artist_name == e.artist and s.duration = e.length)
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table
    print(" ====> Loaded the data into the Songplays Table of Log_Data.")

def main():
    spark = create_spark_session()
    input_data = config['TEST']
    output_data = config['TEST']['output_data']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
