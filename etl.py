import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['Keys']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['Keys']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    create a spark session
    """
    spark = SparkSession \
        .builder \
        .appName('spark_data_lake')\
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
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

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
    Function: Extract data from log_data files for user and time tables. From both log_data and song_data     files get data for songplays     table. Data written into parquet files and load into S3 bucket
    
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
                   .select('ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'length', 'location', 'userAgent')

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
                    .parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    df_songs = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    df_song_actions = df_actions.join(df_songs, (df_actions['artist'] == df_songs['artist_name'])
                                  & (df_actions['song'] == df_songs['title'])
                                  & (df_actions['length'] == df_songs['duration']),'inner')
    
    songplays_table = df_song_actions.select(
        col('datetime').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    ).withColumn('songplay_id', monotonically_increasing_id())
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.createOrReplaceTempView('songplays')
    
    time_table = time_table.alias('timetable')
    
    songplays_table.write.partitionBy(
        'year', 'month'
    ).parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')


def main():
    '''
    Funtions:
    * Get or create a spark session
    * Read the song and log data from S3
    * Transform data into dimension and fact tables
    * Wrote into parquet files
    * Load parquet files to S3
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
#     input_data = "./data/"
    output_data = "s3a://lj-data-store/spark-data-lake-demo/"
#     output_data = "./data/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
