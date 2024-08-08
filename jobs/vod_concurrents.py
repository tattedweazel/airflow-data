from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'vod_concurrents'

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
    else:
        segment_source = 'WCwpjnzJCv'

    s3_path = f's3://{s3_source_bucket}/segment-data/data/{segment_source}/segment_type=video_heartbeat/year={run_year}/month={run_month}/day={run_day}/'

    try:
        return spark.read.option("mergeSchema", "true").parquet(s3_path)
    except:
        return None


def get_platform_events(platform):
    platform_lower = platform.lower()
    heartbeat_data_df = load_event_data(platform_lower)

    if not heartbeat_data_df:
        return None

    heartbeat_data_df = heartbeat_data_df.withColumn('time', heartbeat_data_df['received_at'])
    windowed = heartbeat_data_df.groupBy(F.window("time", "1 minute")).agg(F.count("received_at").alias("count"))

    return windowed.withColumn("month",
                               F.format_string("%02d", F.month(windowed.window.start))) \
        .withColumn("day",
                    F.format_string("%02d", F.dayofmonth(windowed.window.start))) \
        .withColumn("year", F.year(windowed.window.start)) \
        .selectExpr(
            f"CONCAT(window.start, '{platform_lower}') as session_id",
            "window.start as start_timestamp",
            "CAST(count / 2 as INT) as count",  # because we're doing this by the minute, but we have 30-second heartbeats
            f"'{platform_lower}' as platform",
            "year(window.start) as year",
            "month",
            "day"
        )


def write_to_s3(dataframe, object_name):

    partitions = ["year", "month", "day"]
    dataframe.coalesce(8) \
        .write \
        .partitionBy(partitions) \
        .mode('overwrite') \
        .parquet(f"s3://{s3_dest_bucket}/{object_name}")


## Create Roll-ups
android_cleaned = get_platform_events('Android')
ios_cleaned = get_platform_events('iOS')
web_cleaned = get_platform_events('Web')
appletv_cleaned = get_platform_events('lr_apple')
firetv_cleaned = get_platform_events('lr_fire')
roku_cleaned = get_platform_events('lr_roku')
xbox_cleaned = get_platform_events('lr_xbox')

## Union platforms into single dataframe
all_records = None
for df in [android_cleaned, ios_cleaned, web_cleaned, appletv_cleaned, firetv_cleaned, roku_cleaned, xbox_cleaned]:
    if all_records is None and df is not None:
        all_records = df
    else:
        if df is not None:
            all_records = all_records.union(df)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
