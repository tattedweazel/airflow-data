from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

event_name = 'subscription'

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


def load_event_data():
      segment_source = 'NtmpnPjgdx'

      s3_path = f's3://{s3_source_bucket}/segment-data/data/{segment_source}/segment_type={event_name}/year={run_year}/month={run_month}/day={run_day}/'

      try:
            return spark.read.option("mergeSchema", "true").parquet(s3_path)
      except:
            return None

def get_aggregate(factsubscriptions_data, dim_users):
    """ Takes in dataframes with all platform heartbeats, Heartbeat
        counts by Session, and Session Start start times
        then aggregates all together in final structure """

    if not 'coupon_code' in factsubscriptions_data.columns:
        factsubscriptions_data = factsubscriptions_data.withColumn("coupon_code",F.lit(None).cast("string"))

    return factsubscriptions_data.alias("ss").join(dim_users.alias("du"),
            F.col("ss.user_id") == F.col("du.user_id"),
            "left_outer")\
        .withColumn("month",
            F.format_string("%02d",F.month(F.col("ss.received_at"))))\
        .withColumn("day",
            F.format_string("%02d",F.dayofmonth(F.col("ss.received_at"))))\
        .selectExpr("du.user_key as user_key",
            "ss.id as id",
            "ss.subscription_id as subscription_id",
            "ss._event as subscription_event",
            "ss.event_type as subscription_event_type",
            "cast(ss.start_timestamp as timestamp) as start_timestamp",
            "cast(ss.end_timestamp as timestamp) as end_timestamp",
            "coalesce(cast(ss.original_timestamp as timestamp),cast(ss.received_at as timestamp)) as event_timestamp",
            "cast(ss.trial as boolean) as trial",
            "ss.payment_provider as payment_provider",
            "ss.membership_plan as membership_plan",
            "cast(ss.amount_cents as integer) as amount_cents",
            "ss.coupon_code as coupon_code",
            "cast(ss.stripe_user as boolean) as stripe_user",
            "cast(ss.sent_at as timestamp) as sent_at",
            "ss.user_id as user_id",
            "year(ss.received_at) as year",
            "month",
            "day")\
        .dropDuplicates(subset=['subscription_id','subscription_event','subscription_event_type'])\
        .na.drop(subset=["subscription_id","subscription_event","subscription_event_type"])


def perform_rollup(dim_users):
    """ Takes in proper cased name of platform and returns cleaned dataframe """

    factsubscriptions_data_df = load_event_data()

    if not factsubscriptions_data_df:
            return None

    return get_aggregate(
        factsubscriptions_data_df,
        dim_users
    )


def write_to_s3(dataframe, object_name):
      partitions = ["year", "month", "day"]
      dataframe.coalesce(8) \
            .write\
            .partitionBy(partitions)\
            .mode('overwrite')\
            .parquet(f"s3://{s3_dest_bucket}/{object_name}")


def get_dim(dim_name):
    return spark.read.parquet(f"s3://{s3_dest_bucket}/{dim_name}").select('user_key', 'user_id')


## Get Dim Tables
dim_users = get_dim('dim_user')

## Create Roll-ups
all_records = perform_rollup(dim_users)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
