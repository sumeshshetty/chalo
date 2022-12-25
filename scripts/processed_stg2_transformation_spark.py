import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import when, col, hour
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql import types as t

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


s3_output_path_gps_dispatch_stop_time = 's3://chalo-quess-ml-test/glue_processed/processed/gps_dispatch_stop_time_info'

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "processed_gps_dispatch_stop_info", transformation_ctx = "datasource0")

applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("timestamp", "string", "timestamp_val", "bigint"), ("stop_id", "string", "stop_id", "string"), ("name", "string", "name", "string"), ("distance", "double", "distance", "double"), ("routeid", "string", "routeid", "string")], transformation_ctx = "applymapping1")

joined_df = applymapping1.toDF()

joined_df = joined_df.withColumn("is_stop", \
   when((joined_df.distance < 0.03), True).otherwise(False) \
  )
  
joined_df = joined_df.where(joined_df.is_stop)


joined_df = joined_df.withColumn("df_timestamp", from_unixtime((joined_df.timestamp_val/1000)))\
            .withColumn("metric_hour", hour(col("df_timestamp")))\
            .withColumn("metric_month", month(col("df_timestamp")))\
            .withColumn("metric_day", dayofmonth(col("df_timestamp")))\
            .withColumn("metric_year", year(col("df_timestamp"))).drop("df_timestamp")



joined_df.show(truncate = False)

  
  
joined_df.write.mode("overwrite").partitionBy("metric_year","metric_month","metric_day","metric_hour","routeid").parquet(s3_output_path_gps_dispatch_stop_time)
job.commit()





