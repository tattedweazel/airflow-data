from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'staging'

input_event_name = 'hero_event'
output_event_name = 'growthbook_test_hero_event'

spark = SparkSession.builder.appName(input_event_name).getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

## This is the block that will need to be paramaterized
run_year = sys.argv[1]
run_month = sys.argv[2]
run_day = sys.argv[3]
s3_source_bucket = f"segment-rtd-{environment}-s3-data-lake"
s3_dest_bucket = 'airflow-rt-data/processed'
## End of list of paramaters


def load_event_data(platform):

    if platform == 'web_staging':
        segment_source = 'qvwPuJmpU6'
    elif platform == 'web':
        segment_source = 'QoIWHsoWbK'
    elif platform == 'ios':
        segment_source = 'tVPKrSnQ8s'
    else:
        segment_source = 'WCwpjnzJCv'


    s3_path = f's3://{s3_source_bucket}/segment-data/data/{segment_source}/segment_type={input_event_name}/year={run_year}/month={run_month}/day={run_day}/'

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

    if 'item_use' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('item_use', F.lit(None).cast("string"))

    if 'url' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('url', F.lit(None).cast("string"))

    if 'has_sso' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('has_sso', F.lit(None).cast("string"))

    if 'has_video' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('has_video', F.lit(None).cast("string"))

    return event_data_df.alias("ed") \
        .withColumn("month",
                    F.format_string("%02d", F.month(F.col("ed.received_at")))) \
        .withColumn("day",
                    F.format_string("%02d", F.dayofmonth(F.col("ed.received_at")))) \
        .withColumn('user_tier',
                    F.when((F.col("ed.user_tier").isNull()) & (F.col("ed.user_id").isNull()), "anon")
                    .when((F.col("ed.user_tier").isNull()) & (F.col("ed.user_id").isNotNull()), "free")
                    .when((F.lower(F.col("ed.user_tier")) == 'free') & (F.col("ed.user_id").isNull()), "anon")
                    .otherwise(F.lower(F.col("ed.user_tier")))) \
        .selectExpr("ed.id as event_id",
                    "ed.user_id as user_id",
                    "ed.anonymous_id as anonymous_id",
                    "user_tier",
                    "ed.item_type as item_type",
                    "ed.item_name as item_name",
                    "ed.item_use as item_use",
                    "ed.url as url",
                    "ed.cta_type as cta_type",
                    "ed.has_sso as has_sso",
                    "ed.has_video as has_video",
                    f"'{platform_lower}' as platform",
                    "ed.received_at as event_timestamp",
                    "year(ed.received_at) as year",
                    "month",
                    "day") \
        .dropDuplicates(subset=['event_id'])


def write_to_s3(dataframe, object_name):

    partitions = ["year", "month", "day"]
    dataframe.coalesce(8) \
            .write\
            .partitionBy(partitions)\
            .mode('overwrite')\
            .parquet(f"s3://{s3_dest_bucket}/{object_name}")


## Create Roll-ups
web_staging_cleaned = get_platform_events('web_staging')

## Union platforms into single dataframe
all_records = web_staging_cleaned  # Only on web staging

## Write records to fact table
if all_records:
    write_to_s3(all_records, output_event_name)
