from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'fact_visits'

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

    if platform == 'web':
        segment_type = 'pages'
    else:
        segment_type = 'screens'

    s3_path = f's3://{s3_source_bucket}/segment-data/data/{segment_source}/segment_type={segment_type}/year={run_year}/month={run_month}/day={run_day}/'

    try:
        return spark.read.option("mergeSchema", "true").parquet(s3_path)
    except:
        return None


def get_platform_events(platform, dim_users):
    platform_lower = platform.lower()
    factvisits_data_df = load_event_data(platform_lower)

    if not factvisits_data_df:
        return None

    # if there's no user id column at all, add one with Null values
    if 'user_id' not in factvisits_data_df.columns and 'user_uuid' not in factvisits_data_df.columns:
        factvisits_data_df = factvisits_data_df.withColumn("user_uuid", F.lit(None).cast("string"))

    if 'context_user_agent' not in factvisits_data_df.columns:
        factvisits_data_df = factvisits_data_df.withColumn("context_user_agent", F.lit(None).cast("string"))

    # some platforms use user_id, others user_uuid
    if 'user_id' in factvisits_data_df.columns:
        uuid_column = 'user_id'
    else:
        uuid_column = 'user_uuid'

    return factvisits_data_df.alias("fv").\
        join(dim_users.alias("du"),
             F.col("fv.{}".format(uuid_column)) == F.col("du.user_id"),
             "left_outer") \
        .where(
        (
                ~F.col("fv.context_user_agent").rlike("bot") &
                ~F.col("fv.context_user_agent").rlike("spider") &
                ~F.col("fv.context_user_agent").rlike("crawl")
        ) |
        F.col("fv.context_user_agent").isNull()) \
        .withColumn("month", F.format_string("%02d", F.month(F.col("fv.received_at")))) \
        .withColumn("day", F.format_string("%02d", F.dayofmonth(F.col("fv.received_at")))) \
        .selectExpr("fv.id as message_id",
                    "fv.anonymous_id as anonymous_id",
                    "fv.received_at as timestamp",
                    f"'{platform_lower}' as platform",
                    "du.user_key as user_key",
                    "year(fv.received_at) as year",
                    "month",
                    "day"
                    ) \
        .dropDuplicates(subset=['message_id']) \
        .na.drop(subset=["message_id", "anonymous_id"])


def write_to_s3(dataframe, object_name):

    partitions = ["year", "month", "day"]
    dataframe.coalesce(8) \
        .write \
        .partitionBy(partitions) \
        .mode('overwrite') \
        .parquet(f"s3://{s3_dest_bucket}/{object_name}")


def get_dim(dim_name):
    return spark.read.parquet(f"s3://{s3_dest_bucket}/{dim_name}").select('user_key', 'user_id')


## Get Dim Tables
dim_users = get_dim('dim_user')

## Create Roll-ups
android_cleaned = get_platform_events('Android', dim_users)
ios_cleaned = get_platform_events('iOS', dim_users)
web_cleaned = get_platform_events('Web', dim_users)
appletv_cleaned = get_platform_events('lr_apple', dim_users)
firetv_cleaned = get_platform_events('lr_fire', dim_users)
roku_cleaned = get_platform_events('lr_roku', dim_users)
xbox_cleaned = get_platform_events('lr_xbox', dim_users)
androidtv_cleaned = get_platform_events('lr_android', dim_users)

## Union platforms into single dataframe
all_records = None
for df in [android_cleaned, ios_cleaned, web_cleaned, appletv_cleaned, firetv_cleaned, roku_cleaned, xbox_cleaned, androidtv_cleaned]:
    if all_records is None and df is not None:
        all_records = df
    else:
        if df is not None:
            all_records = all_records.union(df)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
