import os
import argparse
import configparser
from datetime import datetime

from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config["aws"]["AWS_ACCESS_KEY_ID"]
os.environ['AWS_SECRET_ACCESS_KEY'] = config["aws"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')

    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView('song_data')

    # extract columns to create songs table
    songs_table = spark.sql(
        """
        select song_id, title, artist_id, year, duration from song_data
        """
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = spark.sql(
        """
        select
            artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
        from song_data
        """
    )

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays and  create timestamp column from original timestamp column
    spark.udf.register("get_datetime", lambda x: datetime.fromtimestamp(x / 1000.0).isoformat())
    df.createOrReplaceTempView('log_data')

    df = spark.sql(
        """
        select
            *,
            cast(userId as int) as userIdInt,
            get_datetime(ts) as start_time
        from log_data
        where page='NextSong'
        """
    )

    df.createOrReplaceTempView('log_data')

    # extract columns for users table
    users_table = spark.sql(
        """
        select user_id, first_name, last_name, gender, level
        from
        (
            select
                userIdInt as user_id,
                firstName as first_name,
                lastName as last_name,
                gender,
                level,
                ts,
                row_number() over (partition by userIdInt ORDER BY ts DESC) as row_num
            from log_data
        ) where row_num = 1    
        """
    )

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, 'users'))

    # extract columns to create time table
    time_table = spark.sql(
        """
        select 
            start_time,
            year(start_time) as year,
            month(start_time) as month,
            day(start_time) as day,
            hour(start_time) as hour,
            weekofyear(start_time) as week,
            dayofweek(start_time) as weekday
        from log_data
        """
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(os.path.join(output_data, 'time'), partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    songs_df = spark.read.json(os.path.join(input_data, 'song_data', '*', '*', '*'))

    # extract columns from joined song and log datasets to create songplays table
    df = spark.sql(
        """
        select monotonically_increasing_id() as songplay_id, * 
        from log_data
        order by ts
        """
    )

    songs_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('events')

    songplays_table = spark.sql(
        """
        select
            e.songplay_id,
            e.start_time,
            e.userIdInt as user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent,
            year(e.start_time) as year,
            month(e.start_time) as month
        from events e
        left join songs s on
            e.song = s.title 
            and e.artist = s.artist_name
            and abs(e.length - s.duration) < 1
        """
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])


def main(args):
    spark = create_spark_session()
    if not args.local:
        input_data = "s3a://udacity-dend/"
        output_data = ""
    else:
        input_data, output_data = 'data', 'output'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--local', dest='local', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
