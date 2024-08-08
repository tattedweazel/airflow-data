from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'playlist_event'

event_playlist_viewed_label = 'playlist_viewed'
event_item_watched_label = 'playlist_item_watched'
event_item_added_label = 'playlist_item_added'

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


## End of list of paramaters

# Methods
def load_event_data(platform, event_label):
    """ Web, iOS, Android """
    if platform == 'web':
        segment_source = 'QoIWHsoWbK'
    elif platform == 'ios':
        segment_source = 'tVPKrSnQ8s'
    else:
        segment_source = 'WCwpjnzJCv'

    s3_path = f's3://{s3_source_bucket}/segment-data/data/{segment_source}/segment_type={event_label}/year={run_year}/month={run_month}/day={run_day}/'

    try:
        return spark.read.option("mergeSchema", "true").parquet(s3_path)
    except:
        return None


def get_playlist_viewed_platform_events(platform, event_label):
    platform_lower = platform.lower()
    event_data_df = load_event_data(platform_lower, event_label)

    if not event_data_df:
        return None

    return event_data_df.alias("ed") \
        .withColumn("item_id", F.lit(None).cast("string")) \
        .withColumn("item_type", F.lit(None).cast("string")) \
        .withColumn("month",
                 F.format_string("%02d",F.month(F.col("ed.received_at"))))\
        .withColumn("day",
                 F.format_string("%02d",F.dayofmonth(F.col("ed.received_at"))))\
        .selectExpr("ed.anonymous_id as anonymous_id",
                    "item_id",
                    "item_type",
                    "'Playlist Viewed' as label",
                    "ed.id as message_id",
                    "ed.platform as platform",
                    "ed.playlist_id as playlist_id",
                    "ed.playlist_name as playlist_name",
                    "ed.received_at as timestamp",
                    "ed.user_id as user_id",
                    "year(ed.received_at) as year",
                    "month",
                    "day") \
        .dropDuplicates(subset=['message_id'])


def get_playlist_item_watched_platform_events(platform, event_label):
    platform_lower = platform.lower()
    event_data_df = load_event_data(platform_lower, event_label)

    if not event_data_df:
        return None

    return event_data_df.alias("ed") \
        .withColumn("month",
                 F.format_string("%02d",F.month(F.col("ed.received_at"))))\
        .withColumn("day",
                 F.format_string("%02d",F.dayofmonth(F.col("ed.received_at"))))\
        .selectExpr("ed.anonymous_id as anonymous_id",
                    "ed.item_id as item_id",
                    "ed.item_type as item_type",
                    "'Playlist Item Watched' as label",
                    "ed.id as message_id",
                    "ed.platform as platform",
                    "ed.playlist_id as playlist_id",
                    "ed.playlist_name as playlist_name",
                    "ed.received_at as timestamp",
                    "ed.user_id as user_id",
                    "year(ed.received_at) as year",
                    "month",
                    "day") \
        .dropDuplicates(subset=['message_id'])


def get_playlist_item_added_platform_events(platform, event_label):
    platform_lower = platform.lower()
    event_data_df = load_event_data(platform_lower, event_label)

    if not event_data_df:
        return None

    if 'item_id' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('item_id', F.lit(None).cast("string"))

    return event_data_df.alias("ed") \
        .withColumn("month",
                 F.format_string("%02d",F.month(F.col("ed.received_at"))))\
        .withColumn("day",
                 F.format_string("%02d",F.dayofmonth(F.col("ed.received_at"))))\
        .selectExpr("ed.anonymous_id as anonymous_id",
                    "ed.item_id as item_id",
                    "ed.item_type as item_type",
                    "'Playlist Item Added' as label",
                    "ed.id as message_id",
                    "ed.platform as platform",
                    "NULL as playlist_id",
                    "NULL as playlist_name",
                    "ed.received_at as timestamp",
                    "ed.user_id as user_id",
                    "year(ed.received_at) as year",
                    "month",
                    "day") \
        .dropDuplicates(subset=['message_id'])


def write_to_s3(dataframe, object_name):

        partitions = ["year", "month", "day"]
        dataframe.coalesce(8) \
                .write\
                .partitionBy(partitions)\
                .mode('overwrite')\
                .parquet(f"s3://{s3_dest_bucket}/{object_name}")


playlist_viewed = get_playlist_viewed_platform_events('Web',event_playlist_viewed_label)
playlist_item_watched = get_playlist_item_watched_platform_events('Web',event_item_watched_label)
playlist_item_added = get_playlist_item_added_platform_events('Web',event_item_added_label)

## Union platforms into single dataframe
all_records = None
for df in [playlist_viewed, playlist_item_watched, playlist_item_added]:
    if all_records is None and df is not None:
        all_records = df
    else:
        if df is not None:
            all_records = all_records.union(df)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
