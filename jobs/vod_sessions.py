from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'vod_sessions'

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
    elif platform == 'ios':
        segment_source = 'tVPKrSnQ8s'
    elif platform == 'lr_roku':
        segment_source = 'vYnWYfQXdPu82NYN4xB3or'
    elif platform == 'lr_fire':
        segment_source = '3S7cr7ZP4LPQwanZptak4X'
    elif platform == 'lr_xbox':
        segment_source = '82QUauRB2fcLwFdhxrBkPc'
    elif platform == 'lr_apple':
        segment_source = 'kCuGxxttj19mqUtbiKLHyx'
    elif platform == 'lr_android':
        segment_source = 'kVJbcWwpbnqj9z2iT22ew6'
    else:
        segment_source = 'WCwpjnzJCv'

    s3_path = f's3://{s3_source_bucket}/segment-data/data/{segment_source}/segment_type=video_heartbeat/year={run_year}/month={run_month}/day={run_day}/'

    try:
        return spark.read.option("mergeSchema", "true").parquet(s3_path)
    except:
        return None


def get_heartbeat_aggregates(heartbeat_data, platform):

    if platform in ('web', 'lr_roku', 'lr_fire', 'lr_xbox', 'lr_apple', 'lr_android'):
        if not 'user_uuid' in heartbeat_data.columns:
            heartbeat_data = heartbeat_data.withColumn("user_uuid", F.lit(None).cast("string"))
        uuid_column = 'user_uuid'
    else:
        uuid_column = 'user_id'

    if 'on_mobile_device' not in heartbeat_data.columns and platform in ('ios', 'android'):
        heartbeat_data = heartbeat_data.withColumn('on_mobile_device', F.lit(1).cast(BooleanType()))
    elif 'on_mobile_device' not in heartbeat_data.columns and platform in ('lr_roku', 'lr_fire', 'lr_xbox', 'lr_apple', 'lr_android'):
        heartbeat_data = heartbeat_data.withColumn('on_mobile_device', F.lit(0).cast(BooleanType()))
    elif 'on_mobile_device' not in heartbeat_data.columns:
        heartbeat_data = heartbeat_data.withColumn('on_mobile_device', F.lit(None).cast(BooleanType()))


    return heartbeat_data.groupby('session_id', 'content_id', 'anonymous_id', 'user_tier', uuid_column, 'on_mobile_device').count()


def get_counts_by_session(heartbeat_data):
    """ Takes in full heartbeat_data dataframe and returns a
        dataframe containing heartbeats counts for each Session ID """

    return heartbeat_data.groupby('session_id') \
        .agg(F.count('*').alias("number_of_heartbeats"))


def get_session_starts(heartbeat_data):
    """ Takes in full heartbeat_data dataframe and returns dataframe
        containing the actual start time for each Session ID """

    return heartbeat_data.groupby('session_id') \
        .agg(F.min("received_at").alias("start_timestamp"))


def get_session_ends(heartbeat_data):
    """ Takes in full heartbeat_data dataframe and returns dataframe
        containing the actual end time for each Session ID """

    return heartbeat_data.groupby('session_id') \
        .agg(F.max("received_at").alias("end_timestamp"))


def get_max_positions(heartbeat_data):
    """ Takes in full heartbeat_data dataframe and returns dataframe
        containing the max position reached for each Session ID """

    return heartbeat_data.groupby('session_id') \
        .agg(F.max("position").alias("max_position"))


def get_platform_aggregate(heartbeat_base, counts_by_session, session_starts, session_ends, max_positions, dim_users, platform):
    """ Takes in dataframes with all platform heartbeats, Heartbeat
        counts by Session, and Session Start start times
        then aggregates all together in final structure """

    if platform in ('web', 'lr_roku', 'lr_fire', 'lr_xbox', 'lr_apple', 'lr_android'):
        uuid_column = 'user_uuid'
    else:
        uuid_column = 'user_id'


    return heartbeat_base.alias("hb").\
        join(session_starts.alias("ss"),
             F.col("hb.session_id") == F.col("ss.session_id"),
             "left_outer")\
        .join(counts_by_session.alias("cbs"),
              F.col("hb.session_id") == F.col("cbs.session_id"),
              "left_outer") \
        .join(session_ends.alias("se"),
              F.col("hb.session_id") == F.col("se.session_id"),
              "left_outer") \
        .join(max_positions.alias("mp"),
              F.col("hb.session_id") == F.col("mp.session_id"),
              "left_outer") \
        .join(dim_users.alias("du"),
              F.col("hb.{}".format(uuid_column)) == F.col("du.user_id"),
              "left_outer") \
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
        .withColumn('duration_seconds',
                    F.unix_timestamp("se.end_timestamp") - F.unix_timestamp('ss.start_timestamp') + 30) \
        .selectExpr("CONCAT(hb.session_id, CONCAT(year(ss.start_timestamp), CONCAT(month, day))) as session_id",
                    "du.user_key as user_key",
                    "user_tier",
                    "hb.anonymous_id as anonymous_id",
                    "hb.content_id as episode_key",
                    "(cbs.number_of_heartbeats * 30) as active_seconds",
                    "ss.start_timestamp as start_timestamp",
                    "se.end_timestamp as end_timestamp",
                    "duration_seconds",
                    "mp.max_position as max_position",
                    f"'{platform}' as platform",
                    "hb.on_mobile_device",
                    "year(ss.start_timestamp) as year",
                    "month",
                    "day") \
        .dropDuplicates(subset=['session_id']) \
        .na.drop(subset=["session_id"]) \
        .na.drop(subset=["episode_key"])


def perform_platform_rollup(platform, dim_users):
    """ Takes in proper cased name of platform and returns cleaned dataframe """

    platform_lower = platform.lower()

    # Try to load events for this platform, and if none are found, return None
    try:
        heartbeat_data_df = load_event_data(platform_lower)
    except:  # This exception typically happens when there is no data for that platform on a given day
        return None

    heartbeat_base = get_heartbeat_aggregates(heartbeat_data_df, platform_lower)
    counts_by_session_df = get_counts_by_session(heartbeat_data_df)
    session_starts_df = get_session_starts(heartbeat_data_df)
    session_ends_df = get_session_ends(heartbeat_data_df)
    max_positions_df = get_max_positions(heartbeat_data_df)
    return get_platform_aggregate(
        heartbeat_base,
        counts_by_session_df,
        session_starts_df,
        session_ends_df,
        max_positions_df,
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
android_cleaned = perform_platform_rollup('Android', dim_users)
ios_cleaned = perform_platform_rollup('iOS', dim_users)
web_cleaned = perform_platform_rollup('Web', dim_users)
appletv_cleaned = perform_platform_rollup('lr_apple', dim_users)
firetv_cleaned = perform_platform_rollup('lr_fire', dim_users)
roku_cleaned = perform_platform_rollup('lr_roku', dim_users)
xbox_cleaned = perform_platform_rollup('lr_xbox', dim_users)
androidtv_cleaned = perform_platform_rollup('lr_android', dim_users)

## Union platforms into single dataframe
all_records = None
for df in [android_cleaned, ios_cleaned, web_cleaned, appletv_cleaned, firetv_cleaned, roku_cleaned, xbox_cleaned, androidtv_cleaned]:
    if all_records is None and df is not None:
        all_records = df
    else:
        if df is not None:
            all_records = all_records.union(df)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
