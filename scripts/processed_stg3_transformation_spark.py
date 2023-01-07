import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#args['s3_input_path_gps_dispatch_stop_time'] = 's3://chalo-quess-ml-test/glue_processed/processed/test/gps_dispatch_stop_time_info' 
s3_output_path_gps_dispatch_stop_time = 's3://chalo-quess-ml-test/glue_processed/processed/time_taken_v5'

route_stop_sequence_dyf = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "processed_route_stop_sequence", transformation_ctx = "datasource0")
gps_dispatch_stop_time_dyf = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "processed_gps_dispatch_stop_time_info", transformation_ctx = "gps_dispatch_stop_time_dyf")





route_stop_sequence_df= route_stop_sequence_dyf.toDF().withColumnRenamed("distance","distance_stops")
gps_dispatch_stop_time_df= gps_dispatch_stop_time_dyf.toDF()


source_joined_df = route_stop_sequence_df.join(gps_dispatch_stop_time_df, (route_stop_sequence_df.route_id == gps_dispatch_stop_time_df.routeid)
               & (route_stop_sequence_df.source == gps_dispatch_stop_time_df.stop_id),"left").drop('routeid','stop_id','distance').withColumnRenamed("timestamp_val","timestamp_source")




source_joined_df.createOrReplaceTempView('source_stop_info_table')
gps_dispatch_stop_time_df.createOrReplaceTempView('gps_dispatch_stop_table')

source_destination_time_df = spark.sql(
'''
select b.vehicle_no,a.route_id, a.destination,a.source, a.metric_year, a.metric_month, a.metric_day, a.metric_hour, b.timestamp_val as timestamp_destination,a.timestamp_source,a.distance_stops

from source_stop_info_table a
left join gps_dispatch_stop_table b
on a.route_id = b.routeid and
a.destination = b.stop_id and
a.metric_year = b.metric_year and
a.metric_month = b.metric_month and
a.metric_day = b.metric_day and
a.metric_hour = b.metric_hour and
a.vehicle_no = b.vehicle_no

'''
    )

source_destination_time_df= source_destination_time_df.withColumn("time_taken",(source_destination_time_df.timestamp_destination -source_destination_time_df.timestamp_source)/1000/60 ).drop('timestamp_destination','timestamp_source')


source_destination_time_df = source_destination_time_df.filter(col("time_taken").isNotNull())

source_destination_time_df= source_destination_time_df.withColumn("speed",source_destination_time_df.distance_stops / (source_destination_time_df.time_taken/60)) 


source_destination_time_df.coalesce(1).write.options(header='True', delimiter=',').mode("overwrite").partitionBy("metric_year","metric_month","metric_day").csv(s3_output_path_gps_dispatch_stop_time)

job.commit()






