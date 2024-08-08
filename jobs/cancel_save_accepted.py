from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# environment is either 'prod' or 'staging'
environment = 'prod'

# event_name is the lower_snake_cased version of the Event (ClickEvent -> click_event, Livestream Heartbeat -> livestream_heartbeat)
event_name = 'cancel_save_accepted'

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
def load_event_data():
        """ These events only come from one source - Backend """
        segment_source = 'NtmpnPjgdx'
            
        s3_path = f's3://{s3_source_bucket}/segment-data/data/{segment_source}/segment_type={event_name}/year={run_year}/month={run_month}/day={run_day}/'

        try:
            return spark.read.option("mergeSchema", "true").parquet(s3_path)
        except:
            return None


def get_platform_events():
        event_data_df = load_event_data()

        if not event_data_df:
            return None

        return event_data_df.alias("ed")\
                .withColumn("month",
			             F.format_string("%02d",F.month(F.col("ed.received_at"))))\
		        .withColumn("day",
			             F.format_string("%02d",F.dayofmonth(F.col("ed.received_at"))))\
                .selectExpr("ed.id as event_id",
                        "ed.received_at as event_timestamp",
                        "ed.user_id",
                        "ed.coupon_code",
                        "ed.offer_code",
                        "ed.new_plan_code",
                        "ed.new_plan_end",
                        "ed.new_plan_start",
                        "ed.old_plan_code",
                        "ed.old_plan_end",
                        "ed.old_plan_start",
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
records = get_platform_events()

## Write records to fact table
if records:
    write_to_s3(records, event_name)
