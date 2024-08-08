from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'hero_item_clicked'

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

    if not 'target_name' in event_data_df.columns:
        event_data_df = event_data_df.withColumn("target_name",F.lit(None).cast("string"))

    if not 'target_uuid' in event_data_df.columns:
        event_data_df = event_data_df.withColumn("target_uuid",F.lit(None).cast("string"))

    return event_data_df.alias("ed") \
        .withColumn("month",
                 F.format_string("%02d",F.month(F.col("ed.received_at"))))\
        .withColumn("day",
                 F.format_string("%02d",F.dayofmonth(F.col("ed.received_at"))))\
        .selectExpr("ed.id as message_id",
                    "ed.received_at as timestamp",
                    "ed.user_id as user_id",
                    "ed.anonymous_id as anonymous_id",
                    "ed.location as event_location",
                    "ed.label as event_type",
                    "ed.target_name as item_name",
                    "ed.target_uuid as item_id",
                    "ed.platform as platform",
                    "year(ed.received_at) as year",
                    "month",
                    "day") \
        .na.drop(subset=["timestamp"]) \
        .dropDuplicates(subset=['message_id'])



def write_to_s3(dataframe, object_name):

    partitions = ["year", "month", "day"]
    dataframe.coalesce(8) \
            .write\
            .partitionBy(partitions)\
            .mode('overwrite')\
            .parquet(f"s3://{s3_dest_bucket}/{object_name}")


## Create Roll-ups
web_cleaned = get_platform_events('Web')

## Union platforms into single dataframe
all_records = web_cleaned

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
