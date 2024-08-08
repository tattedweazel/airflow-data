from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'application_opened'

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

        if platform == 'android':
            segment_source = 'WCwpjnzJCv'
        elif platform == 'ios':
            segment_source = 'tVPKrSnQ8s'

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
                .selectExpr("ed.id as event_id",
                        "ed.context_app_version as app_version",
                        "cast(ed.build as string) as build",
                        "ed.context_device_manufacturer as device_manufacturer",
                        "ed.context_device_model as device_model",
                        "ed.context_network_wifi as wifi",
                        "ed.context_network_cellular as cellular",
                        "ed.context_os_name as os_name",
                        "ed.context_os_version as os_version",
                        "ed.from_background as from_background",
                        "ed.received_at as event_timestamp",
                        "'{}' as platform".format(platform),
                        "ed.user_id as user_id",
                        "ed.anonymous_id as anonymous_id",
                        "ed.context_traits_member_tier as user_tier",
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
android_cleaned = get_platform_events('Android')
ios_cleaned = get_platform_events('iOS')

## Union platforms into single dataframe
all_records = android_cleaned.union(ios_cleaned)

## Write records to fact table
if all_records:
    write_to_s3(all_records, event_name)
