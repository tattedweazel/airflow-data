from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, FloatType
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'vod_deciles'

spark = SparkSession.builder.appName(event_name).getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

## This is the block that will need to be paramaterized
run_year = sys.argv[1]
run_month = sys.argv[2]
run_day = sys.argv[3]
s3_source_bucket = f"segment-rtd-{environment}-s3-data-lake"
s3_dest_bucket = 'airflow-rt-data/processed'
## End of list of paramaters

# Methods
def load_event_data(platform):

    if platform == 'web':
        segment_source = 'QoIWHsoWbK'
    else:
        segment_source = 'WCwpjnzJCv'

    s3_path = f's3://{s3_source_bucket}/segment-data/data/{segment_source}/segment_type=video_heartbeat/year={run_year}/month={run_month}/day={run_day}/'

    try:
        return spark.read.option("mergeSchema", "true").parquet(s3_path)
    except:
        return None


def get_session_starts(heartbeat_data):
    """ Takes in full heartbeat_data dataframe and returns dataframe
        containing the actual start time for each Session ID """

    return heartbeat_data.groupby('session_id') \
        .agg(F.min("received_at").alias("start_timestamp"))


def get_platform_aggregate(heartbeat_base, session_starts, dim_users, platform):
    """ Takes in dataframes with all platform heartbeats, Heartbeat
        counts by Session, and Session Start start times
        then aggregates all together in final structure """

    # Set uuid
    if platform in ('web', 'lr_roku', 'lr_fire', 'lr_xbox', 'lr_apple', 'lr_android'):
        if not 'user_uuid' in heartbeat_base.columns:
            heartbeat_base = heartbeat_base.withColumn("user_uuid", F.lit(None).cast("string"))
        uuid_column = 'user_uuid'
    else:
        uuid_column = 'user_id'

    # Set on_mobile_device
    if 'on_mobile_device' not in heartbeat_base.columns and platform in ('ios', 'android'):
        heartbeat_base = heartbeat_base.withColumn('on_mobile_device', F.lit(1).cast(BooleanType()))
    elif 'on_mobile_device' not in heartbeat_base.columns and platform in ('lr_roku', 'lr_fire', 'lr_xbox', 'lr_apple', 'lr_android'):
        heartbeat_base = heartbeat_base.withColumn('on_mobile_device', F.lit(0).cast(BooleanType()))
    elif 'on_mobile_device' not in heartbeat_base.columns:
        heartbeat_base = heartbeat_base.withColumn('on_mobile_device', F.lit(None).cast(BooleanType()))


    return heartbeat_base.alias("hb").\
        join(session_starts.alias("ss"), F.col("hb.session_id") == F.col("ss.session_id"), "leftouter")\
        .join(dim_users.alias("du"), F.col("hb.{}".format(uuid_column)) == F.col("du.user_id"), "leftouter") \
        .withColumn("month",
                    F.format_string("%02d", F.month(F.col("ss.start_timestamp")))) \
        .withColumn("day",
                    F.format_string("%02d", F.dayofmonth(F.col("ss.start_timestamp")))) \
        .withColumn('user_tier',
                    F.when((F.col("hb.user_tier").isNull()) & (F.col("du.user_key").isNull()), "anon")
                    .when(F.col("hb.user_tier").isNull(), "free")
                    .when(F.lower(F.col("hb.user_tier")) == "first", "premium")
                    .when((F.lower(F.col("hb.user_tier")) == "free") & (F.col("du.user_key").isNull()), "anon")
                    .otherwise(F.lower(F.col("hb.user_tier")))) \
        .withColumn('percentage_watched', F.round(F.col("hb.percentage_watched").cast(FloatType()), 4)) \
        .withColumn("pct_watched_int", F.round(F.col("percentage_watched") * 100, 0)) \
        .withColumn("pct_watched_int", F.col("pct_watched_int").cast(IntegerType())) \
        .withColumn("decile",
                    F.when((F.floor(F.col("pct_watched_int") / 10)) == 1, 1)
                    .when((F.floor(F.col("pct_watched_int") / 10)) == 2, 10)
                    .when((F.floor(F.col("pct_watched_int") / 10)) == 3, 100)
                    .when((F.floor(F.col("pct_watched_int") / 10)) == 4, 1000)
                    .when((F.floor(F.col("pct_watched_int") / 10)) == 5, 10000)
                    .when((F.floor(F.col("pct_watched_int") / 10)) == 6, 100000)
                    .when((F.floor(F.col("pct_watched_int") / 10)) == 7, 1000000)
                    .when((F.floor(F.col("pct_watched_int") / 10)) == 8, 10000000)
                    .when((F.floor(F.col("pct_watched_int") / 10)) == 9, 100000000)
                    .when((F.floor(F.col("pct_watched_int") / 10)) == 10, 1000000000)
                    .otherwise(0)) \
        .selectExpr("hb.session_id as session_id",
                    "du.user_key as user_key",
                    "user_tier",
                    "hb.anonymous_id as anonymous_id",
                    "hb.content_id as episode_key",
                    "ss.start_timestamp as start_timestamp",
                    f"'{platform}' as platform",
                    "hb.on_mobile_device",
                    "decile",
                    "year(ss.start_timestamp) as year",
                    "month",
                    "day") \
        .na.drop(subset=["session_id"]) \
        .na.drop(subset=["episode_key"]) \
        .groupby('session_id', 'user_key', 'user_tier', 'anonymous_id', 'episode_key', 'start_timestamp', 'platform', 'on_mobile_device', 'year', 'month', 'day') \
        .agg(F.sumDistinct('decile').alias('decile'))


def perform_platform_rollup(platform, dim_users):
    """ Takes in proper cased name of platform and returns cleaned dataframe """

    platform_lower = platform.lower()

    # Try to load events for this platform, and if none are found, return None
    try:
        heartbeat_data_df = load_event_data(platform_lower)
    except:  # This exception typically happens when there is no data for that platform on a given day
        return None

    session_starts_df = get_session_starts(heartbeat_data_df)
    return get_platform_aggregate(
        heartbeat_data_df,
        session_starts_df,
        dim_users,
        platform_lower
    )


def write_to_s3(dataframe, object_name):
    """ Takes in dataframe and type information to write to S3 """

    partitions = ["year", "month", "day"]
    dataframe.coalesce(8) \
        .write \
        .partitionBy(partitions) \
        .mode('overwrite') \
        .parquet(f"s3://{s3_dest_bucket}/{object_name}")


def get_dim(dim_name):
    return spark.read.parquet(f"s3://{s3_dest_bucket}/{dim_name}").select('user_key', 'user_id')

## Get Dim Tables
dim_users = get_dim('dim_user')

## Create Roll-ups
web_cleaned = perform_platform_rollup('Web', dim_users)

## Union platforms into single dataframe
all_records = web_cleaned

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
