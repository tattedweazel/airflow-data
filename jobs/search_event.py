from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'search_event'

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

    if 'user_tier' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('user_tier', F.lit(None).cast("string"))

    if 'user_id' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('user_id', F.lit(None).cast("string"))

    if 'selected_tab' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('selected_tab', F.lit(None).cast("string"))

    return event_data_df.alias("ed")\
            .withColumn("month",
                F.format_string("%02d",F.month(F.col("ed.received_at"))))\
            .withColumn("day",
                F.format_string("%02d",F.dayofmonth(F.col("ed.received_at"))))\
            .withColumn('user_tier',
                F.when((F.col("ed.user_tier").isNull()) & (F.col("ed.user_id").isNull()), "anon")
                .when((F.col("ed.user_tier").isNull()) & (F.col("ed.user_id").isNotNull()), "free")
                .when((F.lower(F.col("ed.user_tier")) == 'free') & (F.col("ed.user_id").isNull()), "anon")
                .otherwise(F.lower(F.col("ed.user_tier")))) \
            .withColumn('target_type',
                F.when(F.col('ed.target_type') == "show", "series")
                .when(F.col('ed.target_type') == "shows", "series")
                .when(F.col('ed.target_type') == "episode", "episodes")
                .when(F.col('ed.target_type') == "epısode", "episodes")
                .otherwise(F.col("ed.target_type"))) \
            .selectExpr("ed.id as message_id",
                    "substr(ed.label, 1, 64) as event_type",
                    "substr(ed.query_used, 1, 256) as search_term",
                    "ed.target as target_id",
                    "substr(target_type, 1, 32) as target_type",
                    "ed.user_id as user_id",
                    "ed.anonymous_id as anonymous_id",
                    "user_tier",
                    f"'{platform_lower}' as platform",
                    "ed.received_at as timestamp",
                    "ed.selected_tab",
                    "year(ed.received_at) as year",
                    "month",
                    "day")\
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
android_cleaned = get_platform_events('Android')
ios_cleaned = get_platform_events('iOS')

## Union platforms into single dataframe
all_records = android_cleaned.union(ios_cleaned).union(web_cleaned)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)

