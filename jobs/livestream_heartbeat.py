from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'livestream_heartbeat'

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

    s3_path = f's3://{s3_source_bucket}/segment-data/data/{segment_source}/segment_type=livestream_heartbeat/year={run_year}/month={run_month}/day={run_day}/'

    try:
        return spark.read.option("mergeSchema", "true").parquet(s3_path)
    except:
        return None


def get_platform_aggregate(platform):
    platform_lower = platform.lower()
    heartbeat_data_df = load_event_data(platform_lower)

    if not heartbeat_data_df:
        return None

    if 'user_uuid' not in heartbeat_data_df.columns:
        heartbeat_data_df = heartbeat_data_df.withColumn("user_uuid", F.lit(None).cast("string"))

    if 'user_id' not in heartbeat_data_df.columns:
        heartbeat_data_df = heartbeat_data_df.withColumn("user_id", F.lit(None).cast("string"))

    return heartbeat_data_df.alias("hb") \
        .withColumn("month",
                    F.format_string("%02d", F.month(F.col("hb.received_at")))) \
        .withColumn("day",
                    F.format_string("%02d", F.dayofmonth(F.col("hb.received_at")))) \
        .selectExpr("hb.id as heartbeat_id",
                    "hb.session_id",
                    "hb.anonymous_id",
                    "hb.user_id",
                    "hb.user_uuid",
                    "hb.user_tier",
                    "hb.received_at as event_timestamp",
                    f"'{platform_lower}' as platform",
                    "year(hb.received_at) as year",
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
android_cleaned = get_platform_aggregate('Android')
ios_cleaned = get_platform_aggregate('iOS')
web_cleaned = get_platform_aggregate('Web')
appletv_cleaned = get_platform_aggregate('lr_apple')
firetv_cleaned = get_platform_aggregate('lr_fire')
roku_cleaned = get_platform_aggregate('lr_roku')
xbox_cleaned = get_platform_aggregate('lr_xbox')
androidtv_cleaned = get_platform_aggregate('lr_android')

## Union platforms into single dataframe
all_records = None
for df in [android_cleaned, ios_cleaned, web_cleaned, appletv_cleaned, firetv_cleaned, roku_cleaned, xbox_cleaned,
           androidtv_cleaned]:
    if all_records is None and df is not None:
        all_records = df
    else:
        if df is not None:
            all_records = all_records.union(df)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
