from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'feed_item_viewed'

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

        if 'user_id' not in event_data_df.columns:
            event_data_df = event_data_df.withColumn('user_id', F.lit(None).cast("string"))

        return event_data_df.alias("ed")\
                .withColumn("month",
                         F.format_string("%02d",F.month(F.col("ed.received_at"))))\
                .withColumn("day",
                         F.format_string("%02d",F.dayofmonth(F.col("ed.received_at"))))\
                .selectExpr("ed.id as id",
                        "ed.user_id as user_id",
                        "ed.anonymous_id as anonymous_id",
                        "ed.page as page",
                        "ed.carousel_name as carousel_name",
                        "ed.item_type as item_type",
                        "ed.item_name as item_name",
                        "ed.item_uuid as item_uuid",
                        "ed.feed_tier as feed_tier",
                        "ed.received_at as received_at",
                        "ed.sent_at as sent_at",
                        "ed.original_timestamp as original_timestamp",
                        "year(ed.received_at) as year",
                        "month",
                        "day")\
                .dropDuplicates(subset=['id'])
                

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
all_records = web_cleaned  # There aren't any other platforms for this yet

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
