import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# file to read credentials of aws account
config = configparser.ConfigParser()
config.read('dl.cfg')

# credentials for aws account
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """This method will create a Spark Session

    Args:
        None

    Returns:
        SparkSession
    """
    print('Creating SparkSession...')
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """This method will process song data,
    it will read csv files and save it as parquet

    Args:
        spark: A SparkSession
        input_data: Path where csv's files are stored
        output_data: Path where parquet files should be stored

    Returns:
        None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    print('Reading song data...')
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    print('Preparing song table...')
    songs_table = song_df.select(
        song_df['song_id'].alias('id').cast(T.StringType()),
        song_df['title'].cast(T.StringType()),
        song_df['artist_id'].cast(T.StringType()),
        song_df['year'].cast(T.IntegerType()),
        song_df['duration'].cast(T.DoubleType())
    )

    # write songs table to parquet files partitioned by year and artist
    print('Writing song table as parquet...')
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs')

    # extract columns to create artists table
    print('Preparing artists table...')
    artists_table = song_df.select(
        song_df['artist_id'].alias('id').cast(T.StringType()),
        song_df['artist_name'].alias('name').cast(T.StringType()),
        song_df['artist_location'].alias('location').cast(T.StringType()),
        song_df['artist_latitude'].alias('latitude').cast(T.StringType()),
        song_df['artist_longitude'].alias('longitude').cast(T.StringType())
    )

    # write artists table to parquet files
    print('Writing song table as parquet...')
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """This method will process log data,
    it will read csv files and save it as parquet

    Args:
        spark: A SparkSession
        input_data: Path where csv's files are stored
        output_data: Path where parquet files should be stored

    Returns:
        None
    """

    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read log data file
    print('Reading log data...')
    log_df = spark.read.json(log_data)

    # filter by actions for song plays
    print('Filtering user actions on log...')
    log_df = log_df[log_df['page'] == 'NextSong']

    # extract columns for users table
    print('Preparing users table...')
    users_table = log_df.select(
        log_df['userID'].alias('id').cast(T.IntegerType()),
        log_df['firstName'].cast(T.StringType()),
        log_df['lastName'].cast(T.StringType()),
        log_df['gender'].cast(T.StringType()),
        log_df['level'].cast(T.StringType())
    )

    # write users table to parquet files
    print('Writing users table as parquet...')
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    print('Formatting timestamp for time table...')
    get_timestamp = F.udf(lambda t: datetime.fromtimestamp(t / 1000), T.TimestampType())

    time_df = log_df.select(
        log_df['ts']
    )

    time_df = time_df.withColumn('timestamp', get_timestamp(time_df['ts']))

    # extract columns to create time table
    print('Preparing time table...')
    time_table = time_df.select(
        time_df['timestamp'].alias('start_time'),
        F.hour(time_df['timestamp']).alias('hour'),
        F.dayofmonth(time_df['timestamp']).alias('day'),
        F.weekofyear(time_df['timestamp']).alias('week'),
        F.month(time_df['timestamp']).alias('month'),
        F.year(time_df['timestamp']).alias('year'),
        F.dayofweek(time_df['timestamp']).alias('weekday')
    ).distinct()

    # write time table to parquet files partitioned by year and month
    print('Writing time table as parquet...')
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time')

    # read in song data to use for songplays table
    print('Reading song data for create songplays table...')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    print('Registering temp tables...')
    song_df.registerTempTable('songs')
    log_df.registerTempTable('logs')
    time_df.registerTempTable('time')

    print('Preparing songplays table...')
    songplays_table = spark.sql("""
        select
            row_number() over (order by t.timestamp asc) as id,
            t.timestamp as start_time,
            l.userId as user_id,
            l.level,
            s.song_id,
            s.artist_id,
            l.sessionId as session_id,
            l.location,
            l.userAgent as user_agent,
            month(t.timestamp) as month,
            year(t.timestamp) as year
        from
            songs s
        inner join
            logs l on l.artist = s.artist_name
            and l.song = s.title
        inner join
            time t on t.ts = l.ts
    """)

    # write songplays table to parquet files partitioned by year and month
    print('Writing songplays table as parquet...')
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays')


def main():
    """This is the main method, it will run when this
    module starts and execute other methods

    Args:
        None

    Returns:
        None
    """
    print('Starting ETL')
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://nanodegree-dataengineering-us-west-2/sparkify/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()