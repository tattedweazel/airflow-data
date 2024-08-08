from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

event_label = 'page_event'
event_name = 'pages'

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


def ascii_ignore(x):
    if x:
        return x.encode('ascii', 'ignore').decode('ascii')
    else:
        return None


ascii_udf = F.udf(ascii_ignore)


def load_event_data(platform):

    if platform == 'web':
        segment_source = 'QoIWHsoWbK'
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

    if 'on_mobile_device' not in event_data_df.columns and platform_lower in ('ios', 'android'):
        event_data_df = event_data_df.withColumn('on_mobile_device', F.lit(1).cast(BooleanType()))
    elif 'on_mobile_device' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('on_mobile_device', F.lit(None).cast(BooleanType()))

    if 'context_user_agent' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn("context_user_agent", F.lit(None).cast("string"))


    return event_data_df.alias("ed") \
        .where(
        (
                ~F.col("ed.context_user_agent").rlike("bot") &
                ~F.col("ed.context_user_agent").rlike("spider") &
                ~F.col("ed.context_user_agent").rlike("crawl")
        ) |
        F.col("ed.context_user_agent").isNull()) \
        .withColumn("month",
                    F.format_string("%02d", F.month(F.col("ed.received_at")))) \
        .withColumn("day",
                    F.format_string("%02d", F.dayofmonth(F.col("ed.received_at")))) \
        .withColumn('user_tier',
                    F.when((F.col("ed.user_tier").isNull()) & (F.col("ed.user_id").isNull()), "anon")
                    .when((F.col("ed.user_tier").isNull()) & (F.col("ed.user_id").isNotNull()), "free")
                    .when((F.lower(F.col("ed.user_tier")) == 'free') & (F.col("ed.user_id").isNull()), "anon")
                    .otherwise(F.lower(F.col("ed.user_tier")))) \
        .withColumn('title', ascii_udf('ed.title')) \
        .selectExpr("ed.id as page_event_id",
                    "ed.anonymous_id as anonymous_id",
                    "ed.user_id as user_id",
                    "user_tier",
                    "substring(ed.path, 1, 500) as path",
                    "substring(ed.previous_url, 1, 500) as previous_path",
                    "substring(title, 1, 215) as title",
                    "ed.received_at as event_timestamp",
                    "ed.on_mobile_device",
                    "year(ed.received_at) as year",
                    "month",
                    "day") \
        .dropDuplicates(subset=['page_event_id'])




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
    write_to_s3(all_records, event_label)
