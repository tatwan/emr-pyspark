# using SQL
try: # if using EMR to handle the ImportError 
    import configparser
except ImportError:
    from six.moves import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    SparkSession a unified conduit to all Spark operations and data, and it is an entry point to start programming with DataFrame and Dataset.  
    .getOrCreate() option checks if there is an existing SparkSession otherwise it creates a new one
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function that reads and processes (ETL) song data in JSON format from S3 bucket. 
    

    Argumemnts:
        - spark: supplied SparkSession
        - input_data: Input S3 bucket to read from
        - output_data: Output S3 bucket to write to
        
    Returns:
        This function does not return any value, but does create two folders in S3 for Songs table and Artist table. All files written in Parquet format.
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            select distinct
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration 
                            from songs_data
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs"), mode='overwrite')

    # extract columns to create artists table
    artists_table = spark.sql("""
                                select distinct
                                artist_id ,
                                artist_name ,
                                artist_location ,
                                artist_latitude ,
                                artist_longitude 
                                from songs_data
                                """)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artist"), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Function that reads and processes (ETL) log data in JSON format from S3 bucket. 
    

    Argumemnts:
        - spark: supplied SparkSession
        - input_data: Input S3 bucket to read from
        - output_data: Output S3 bucket to write to
        
    Returns:
        This function does not return any value, but does create three folders in S3 for Users table, Time table, and Songsplays table. All files written in Parquet format.
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    user_table = spark.sql("""
                SELECT distinct
                cast(f.userid as smallint) as id,
                f.firstname,
                f.lastname,
                f.gender,
                f.level
                from
                    (SELECT
                    se.userid,
                    se.firstname ,
                    se.lastname ,
                    se.gender,
                    se.level,
                    se.ts
                    from
                    log_data se
                    join(
                        SELECT
                        userid,
                        max(ts) as mts
                        from
                        log_data
                        where
                        userid != ''
                        group by 1
                    ) level_latest on
                    se.userid = level_latest.userid
                    and se.ts = level_latest.mts
                ) f
                """)
    
    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data, "user"), mode='overwrite')

    # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df = 
    
    # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
                        select ts, 
                        from_unixtime(ts/1000, "hh:mm:ss") as start_time,
                        from_unixtime(ts/1000, "yyyy-MM-dd") as date,
                        month(from_unixtime(ts/1000, "yyyy-MM-dd")) as month,
                        year(from_unixtime(ts/1000, "yyyy-MM-dd")) as year,
                        day(from_unixtime(ts/1000, "yyyy-MM-dd")) as day,
                        weekofyear(from_unixtime(ts/1000, "yyyy-MM-dd")) as week,
                        hour(from_unixtime(ts/1000, "hh:mm:ss")) as hour,
                        weekday(from_unixtime(ts/1000, "yyyy-MM-dd")) as weekday
                        from log_data
                        where log_data.page = 'NextSong'
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time"), mode='overwrite')

   # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("songs_data")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
                    select distinct
                    from_unixtime(ts/1000, "hh:mm:ss") as start_time,
                    year(from_unixtime(ts/1000, "yyyy-MM-dd")) as year,
                    month(from_unixtime(ts/1000, "yyyy-MM-dd")) as month,
                    cast(sev.userid as int) as id,
                    sev.level,
                    son.song_id ,
                    son.artist_id ,
                    sev.sessionid,
                    sev.location,
                    sev.useragent
                    from log_data sev
                    left join songs_data son
                    on sev.artist = son.artist_name
                        and sev.length = son.duration
                        and sev.song = son.title
                    where sev.userid != ''
                        and sev.page = 'NextSong'
                    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays"), mode='overwrite')


def main():
    """
    This is the main thread that create the Spark instance (Session), reads AWS credentials and calls process_song_data and process _log_data functions.
    """
    
    spark = create_spark_session()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://atwan-udacity/p4/"
    
    
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
