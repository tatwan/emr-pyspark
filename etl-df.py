# Using DataFrame API

try: # if using EMR to handle the ImportError 
    import configparser
except ImportError:
    from six.moves import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


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
    Function that reads and processes (ETL)  song data in JSON format from S3 bucket. 
    

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

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs"), mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
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
    
        # create timestamp column from original timestamp column
    @udf(TimestampType())
    def conv_ts(ts):
        """
        Function that takes a UNIX timestamp (EPOCH) and converts to DateTime format
        
        Arguments: Takes in a Unix Timestamp (epoch)
        returns: A datetime value
        """
        return datetime.fromtimestamp(ts/1000)
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df = df.withColumn('datetime', conv_ts('ts'))
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
    

    # extract columns to create time table
    time_table = df.select(
                        'ts',
                        'datetime',
                        date_format('datetime','hh:mm:ss').alias('start_time'),
                        year('datetime').alias('year'),
                        month('datetime').alias('month'),
                        dayofmonth('datetime').alias('dayofmonth'),
                        weekofyear('datetime').alias('weekofyear')
                    ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time"), mode='overwrite')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.filter(df.userId != '')\
        .join(song_df, [df.artist == song_df.artist_name, df.length == song_df.duration , df.song == song_df.title], how='left')\
        .dropDuplicates()\
        .select(
        col('userId').alias('id'),
        date_format('datetime','hh:mm:ss').alias('start_time'),
        year('datetime').alias('year'),
        month('datetime').alias('month'),
        'level', 'song_id', 'artist_id', 'sessionId', 'location', 'useragent')
    
    
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
