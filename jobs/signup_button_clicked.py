from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

event_name = 'signup_button_clicked'

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

    if 'context_page_url' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('context_page_url', F.lit(None).cast("string"))

    if 'on_mobile_device' not in event_data_df.columns and platform_lower in ('ios', 'android'):
        event_data_df = event_data_df.withColumn('on_mobile_device', F.lit(1).cast(BooleanType()))
    elif 'on_mobile_device' not in event_data_df.columns:
        event_data_df = event_data_df.withColumn('on_mobile_device', F.lit(None).cast(BooleanType()))

    return event_data_df.alias("ed")\
            .withColumn("month",
                         F.format_string("%02d",F.month(F.col("ed.received_at"))))\
            .withColumn("day",
                         F.format_string("%02d",F.dayofmonth(F.col("ed.received_at"))))\
            .selectExpr("ed.id as event_id",
                        "ed.anonymous_id as anonymous_id",
                        "substring(ed.context_page_url, 1, 500) as context_page_url",
                        "ed.location as location",
                        "ed.received_at as event_timestamp",
                        f"'{platform_lower}' as platform",
                        "ed.on_mobile_device",
                        "year(ed.received_at) as year",
                        "month",
                        "day")\
            .dropDuplicates(subset=['event_id'])


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
all_records = None
for df in [android_cleaned, ios_cleaned, web_cleaned]:
    if all_records is None and df is not None:
        all_records = df
    else:
        if df is not None:
            all_records = all_records.union(df)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
