from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'initial_app_launch'

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

    return event_data_df.alias("ed") \
        .withColumn("month",
                 F.format_string("%02d",F.month(F.col("ed.received_at"))))\
        .withColumn("day",
                 F.format_string("%02d",F.dayofmonth(F.col("ed.received_at"))))\
        .selectExpr("ed.anonymous_id as anonymous_id",
                    "ed.id as message_id",
                    "'{}' as platform".format(platform),
                    "ed.received_at as event_timestamp",
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


## Create Roll-ups
appletv_cleaned = get_platform_events('lr_apple')
firetv_cleaned = get_platform_events('lr_fire')
roku_cleaned = get_platform_events('lr_roku')
xbox_cleaned = get_platform_events('lr_xbox')
androidtv_cleaned = get_platform_events('lr_android')

## Union platforms into single dataframe
all_records = None
for df in [appletv_cleaned, firetv_cleaned, roku_cleaned, xbox_cleaned, androidtv_cleaned]:
    if all_records is None and df is not None:
        all_records = df
    else:
        if df is not None:
            all_records = all_records.union(df)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
