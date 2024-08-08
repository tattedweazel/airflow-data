from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'ad_event'

spark = SparkSession.builder.appName(event_name).getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

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
    else:
        segment_source = 'WCwpjnzJCv'

    s3_path = f's3://{s3_source_bucket}/segment-data/data/{segment_source}/segment_type={event_name}/year={run_year}/month={run_month}/day={run_day}/'

    try:
        return spark.read.option("mergeSchema", "true").parquet(s3_path)
    except:
        return None


def get_platform_events(platform):
    platform_lower = platform.lower()
    event_data_df = load_event_data(platform_lower)

    if not event_data_df:
        return None

    if 'playhead_time' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('playhead_time', F.lit(None).cast("string"))

    if 'break_type' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('break_type', F.lit(None).cast("string"))

    if 'on_mobile_device' not in event_data_df.columns and platform_lower in ('ios', 'android'):
        event_data_df = event_data_df.withColumn('on_mobile_device', F.lit(1).cast(BooleanType()))
    elif 'on_mobile_device' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('on_mobile_device', F.lit(None).cast(BooleanType()))

    if 'user_id' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('user_id', F.lit(None).cast("string"))

    if 'user_tier' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('user_tier', F.lit(None).cast("string"))

    return event_data_df.alias("ed") \
        .withColumn('break_type',
                    F.when(F.col("ed.break_type") == "preroll", "pre-roll")
                    .when(F.col("ed.break_type") == "midroll", "mid-roll")
                    .when(F.col("ed.break_type") == "MIDROLL", "mid-roll")
                    .when(F.col("ed.break_type") == "PREROLL", "pre-roll")
                    .otherwise(F.col("ed.break_type"))) \
        .withColumn("month",
                    F.format_string("%02d", F.month(F.col("ed.received_at")))) \
        .withColumn("day",
                    F.format_string("%02d", F.dayofmonth(F.col("ed.received_at")))) \
        .withColumn('user_tier',
                    F.when((F.col("ed.user_tier").isNull()) & (F.col("ed.user_id").isNull()), "anon")
                    .when((F.col("ed.user_tier").isNull()) & (F.col("ed.user_id").isNotNull()), "free")
                    .when((F.lower(F.col("ed.user_tier")) == 'free') & (F.col("ed.user_id").isNull()), "anon")
                    .otherwise(F.lower(F.col("ed.user_tier")))) \
        .selectExpr("ed.id as ad_event_id",
                    "ed.anonymous_id as anonymous_id",
                    "break_type",
                    "ed.content_id as content_id",
                    "ed.content_type as content_type",
                    "ed.label as event_type",
                    "ed.platform as platform",
                    "cast(ed.playhead_time as double) as playhead_time",
                    "ed.received_at as event_timestamp",
                    "ed.on_mobile_device",
                    "ed.user_id",
                    "user_tier",
                    "year(ed.received_at) as year",
                    "month",
                    "day") \
        .dropDuplicates(subset=['ad_event_id']) \
        .na.drop(subset=["content_id"])



def write_to_s3(dataframe, object_name):

        partitions = ["year", "month", "day"]
        dataframe.coalesce(8) \
                .write\
                .partitionBy(partitions)\
                .mode('overwrite')\
                .parquet(f"s3://{s3_dest_bucket}/{object_name}")


## Create Roll-ups
web_cleaned = get_platform_events('Web')
android_cleaned = get_platform_events('Android')
ios_cleaned = get_platform_events('iOS')

## Union platforms into single dataframe
if web_cleaned and android_cleaned and not ios_cleaned:
    all_records = android_cleaned.union(web_cleaned)
elif web_cleaned and not android_cleaned and ios_cleaned:
    all_records = ios_cleaned.union(web_cleaned)
elif web_cleaned and not android_cleaned and not ios_cleaned:
    all_records = web_cleaned
else:
    all_records = android_cleaned.union(ios_cleaned).union(web_cleaned)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
