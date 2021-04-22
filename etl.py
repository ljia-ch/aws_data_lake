import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function: Fetch data from song_data folder in S3 bucket and extract columns for songs and artists tables. 
    Write data into parquet files and load to S3 bucket
    
    parameter list
    
    spark:        session, spark session has been created. 
    input_data:   string of path, a path point to S3 bucket.
    output_data:  string of path, a path point to destination in S3.
           
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title','artist_id','year', 'duration').dropDuplicates()
              
    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.writes.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')\
                      .withColumnRenamed('artist_name', 'name')\
                      .withColumnRenamed('artist_location', 'location')\
                      .withColumnRenamed('artist_latitude', 'latitude')\
                      .withColumnRenamed('artist_longitude', 'longitude')\
                      .dropDuplicates()
    artists_table.createOrReplaceTempView('artists')
 
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Function: Extract data from log_data files for user and time tables. From both log_data and song_data files get data for songplays     table. Data written into parquet files and load into S3 bucket
    
    Prameter list
    spark:        session, spark session has been created. 
    input_data:   string of path, a path point to S3 bucket.
    output_data:  string of path, a path point to destination in S3.
    
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_actions = df.filter(df.page == 'NextSong')\
                   .select('ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent')\

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df_actions = df_actions.withColumn('timestamp', get_timestamp(df_actions.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df_actions = df_actions.withColumn('datetime', get_datetime(df_actions.ts)) 
    
    # extract columns to create time table
    time_table = df_actions.select('datetime')\
                           .withColumn('start_time', df_actions.datetime)\
                           .withColumn('hour', hour('datetime'))\
                           .withColumn('day', dayofmonth('datetime'))\
                           .withColumn('week', weekofyear('datetime'))\
                           .withColumn('month', month('datetime'))\
                           .withColumn('year', year('datetime'))\
                           .withColumn('weekday', dayofweek('datetime'))\
                           .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month')\
                    .parquet(os.path.join(output_date, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    df_song = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_actions.alias('df_log')
    df_song = df_song.alias('song_df')

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
