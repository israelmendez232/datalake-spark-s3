# Using this ETL script to pull the data from a S3, transform it and output it another bucket. Using PySpark

import os
import configparser
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


## CONFIG.
config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"]=config["CONFIG"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"]=config["CONFIG"]["AWS_SECRET_ACCESS_KEY"]


## SESSION WITH SPARK
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print(" ====> Creating the Spark Session.")
    print(" ====================>")
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
    song_data = input_data["song_data"]
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    print(" ====> Created the Song Table of Song_Data.")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "/songs/songs_table.parquet")
    print(" ====> Divided the data from Songs_Table of Song_Data.")
    
    # extract columns to create artists table
    artists_table = df.selectExpr(
        "artist_id", 
        "artist_name as name", 
        "artist_location as location", 
        "artist_latitude as latitude", 
        "artist_longitude as longitude"
    ).distinct()
    print(" ====> Created the Artists Table of Song_Data.")

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "/artists/artists_table.parquet")
    print(" ====> Divided the data from Artists_Table of Song_Data.")
    print(" ====================>")
    
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
    log_data = input_data["log_data"]

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays and create the log_table
    df = df[df.page == "NextSong"]

    # extract columns for users table    
    users_table = df.selectExpr(
        "userId as user_id", 
        "firstName as first_name", 
        "lastName as last_name", 
        "gender", 
        "level"
    ).distinct()
    print(" ====> Created the Users Table of Log_Data.")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "/users/users_table.parquet")
    print(" ====> Divided the data from Users Table of Log_Data.")
    
    # create timestamp and datetime column from original timestamp column
    get_timestamp = udf(lambda x: int(x / 1000.0),IntegerType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    get_datetime = udf(lambda x: int(x / 1000.0),IntegerType())
    df = df.withColumn("start_time", from_unixtime(df.timestamp))

    # extract columns to create time table
    time_table = df.dropDuplicates(["start_time"]).select("start_time").withColumn("hour", hour(df.start_time)).withColumn("day", date_format(df.start_time,"d")).withColumn("week", weekofyear(df.start_time)).withColumn("month", month(df.start_time)).withColumn("year", year(df.start_time)).withColumn("weekday", date_format(df.start_time,"u"))
    print(" ====> Created the Time Table of Log_Data.")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy(["year","month"]).parquet(output_data + "/time/time_table.parquet")
    print(" ====> Divided the data into the Time Table of Log_Data.") 
    
    # read in song data to use for songplays table
    song_table = spark.read.parquet(output_data + "/songs/songs_table.parquet")
    song_df = df.join(song_table, df.song == song_table.title, how="left")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.selectExpr(
        "start_time",
        "userId as user_id", 
        "level", 
        "song_id", 
        "artist_id", 
        "sessionId as session_id", 
        "location",
        "userAgent as user_agent"
    )
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id()).withColumn("month", month(df.start_time)).withColumn("year", year(df.start_time))
    print(" ====> Created the Songplays Table of Log_Data.")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "/songplays/songplays_table.parquet")
    print(" ====> Divided the data into the Songplays Table of Log_Data.")

def main():
    spark = create_spark_session()
    input_data = config["CLOUD"]
    output_data = config["CLOUD"]["output_data"]
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
