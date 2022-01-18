import configparser
from datetime import datetime
import pyspark.sql.functions as f
from pyspark.sql import types as t
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Method for creating Apache Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Method for loading input data on the json format to SQL tables (songs and artists) to the output parquet files through spark
    """
    
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    print("reading song data file")
    df = spark.read.json(song_data)

    
    # extract columns to create songs table
    print("extracting columns to create songs table")
    df.createOrReplaceTempView("songs_table")
    songs_table = spark.sql("""
                        SELECT song_id, title, artist_id, year, duration
                        FROM songs_table
                        ORDER BY song_id
    """)
    # write songs table to parquet files partitioned by year and artist
    print("writing songs table to parquet files partitioned by year and artist")
    songs_table.write.parquet(output_data + "songs_table.parquet")
   
    # extract columns to create artists table
    print("extracting columns to create artists table")
    df.createOrReplaceTempView("artists_table")
    artists_table = spark.sql("""
                            SELECT artist_id, 
                            artist_name as name,  
                            artist_location as location, 
                            artist_latitude as lattitude, 
                            artist_longitude as longitude
                            FROM artists_table
                            ORDER BY artist_id
    """)
    
    # write artists table to parquet files
    print("writing artists table to parquet files")
    artists_table.write.parquet(output_data + "artists_table.parquet")
    
    print("Song data table processed")


def process_log_data(spark, input_data, output_data):
    """Method for loading input data on the json format to SQL tables (users, time and songplays) to the output parquet files through spark
    """
    
    # get filepath to log data file
    log_data = input_data

    # read log data file
    print("reading log data file")
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    print("extracting columns for users table")
    df.createOrReplaceTempView("users_table")
    users_table = spark.sql("""
                        SELECT userId as user_id, 
                        firstName as first_name,  
                        lastName as last_name, 
                        gender, 
                        level
                        FROM users_table
                        ORDER BY user_id
    """)
    
    # write users table to parquet files
    print("writing users table to parquet files")
    users_table.write.parquet(output_data + "users_table.parquet")

    # create timestamp column from original timestamp column
    print("creating timestamp column from original timestamp column")
    @udf(t.TimestampType())
    def get_timestamp(ts):
        return datetime.fromtimestamp(ts / 1000.0)
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    print("creating datetime column from original timestamp column")
    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    df = df.withColumn("datetime", get_datetime("ts"))

    
    # extract columns to create time table
    print("extracting columns to create time table")
    df.createOrReplaceTempView("time_table")
    time_table = spark.sql("""
        SELECT  DISTINCT datetime AS start_time, 
                         hour(timestamp) AS hour, 
                         day(timestamp)  AS day, 
                         weekofyear(timestamp) AS week,
                         month(timestamp) AS month,
                         year(timestamp) AS year,
                         dayofweek(timestamp) AS weekday
        FROM time_table
        ORDER BY start_time
    """)

    # write time table to parquet files partitioned by year and month
    print("writing time table to parquet files partitioned by year and month")
    time_table.write.parquet(output_data + "time_table.parquet")

    # create view of joined dataframes
    print("creating view of joined dataframes")
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW joined_artists_users 
        AS SELECT *
        FROM time_table
        JOIN artists_table ON artists_table.artist_name = time_table.artist
    """)
    # read in song data to use for songplays table
    # REFERENCE https://stackoverflow.com/questions/16555454/how-to-generate-auto-increment-field-in-select-query
    print("reading  in song data to use for songplays table")
    songplays_table = spark.sql("""
                SELECT  row_number() OVER (ORDER BY userId, sessionId) AS songplay_id, 
                timestamp   AS start_time, 
                userId      AS user_id, 
                level       AS level,
                song_id     AS song_id,
                artist_id   AS artist_id,
                sessionId   AS session_id,
                location    AS location,
                userAgent   AS user_agent
        FROM joined_artists_users
    """)

    # write songplays table to parquet files partitioned by year and month
    print("writing songplays table to parquet files partitioned by year and month")
    songplays_table.write.parquet(output_data + "songplays_table.parquet")
    print("log data processing completed")

def main():
    spark = create_spark_session()
    while True:
        choice = input("Type l for local, a for aws")
        if choice == 'l':
            print("Local Data Sources and Outputs chosen")
            input_data_log = config['LOCAL']['INPUT_DATA_LOG_DATA_LOCAL']
            input_data_song = config['LOCAL']['INPUT_DATA_SONG_DATA_LOCAL']
            output_data = config['LOCAL']['OUTPUT_DATA_LOCAL']
            break
        elif choice == 'a':
            input_data_log = config['AWS']['INPUT_DATA_LOG_DATA_AWS']
            input_data_song = config['AWS']['INPUT_DATA_SONG_DATA_AWS']
            output_data = config['AWS']['OUTPUT_DATA_AWS']
            print("AWS Data Sources and Outputs chosen")
            break
        print("You have chosen an invalid option, please try again")
                
    process_song_data(spark, input_data_song, output_data)   
    process_log_data(spark, input_data_log, output_data)

    
if __name__ == "__main__":
    main()
