import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import DoubleType, StringType, FloatType
from pyspark.sql.functions import udf
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import when, col, hour, from_unixtime, month, dayofmonth, year

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#script location where pulll_source_data_spark dumps
args['s3_input_path_dispatch'] = 's3://chalo-quess-ml-test/glue_processed/raw/dispatch'
args['s3_input_path_gps'] = 's3://chalo-quess-ml-test/glue_processed/raw/gps_raw'
s3_output_path_gps_dispatch_stop_time = 's3://chalo-quess-ml-test/glue_processed/processed/tmp/gps_dispatch_stop_time_info'


def dist(lat1, long1, lat2, long2):
    lat1, long1, lat2, long2 = map(radians, [lat1, long1, lat2, long2])
    # haversine formula 
    dlon = long2 - long1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return km

def find_nearest(lat, long):
    distances = stops_pdf.apply(
        lambda row: dist(lat, long, row['lat_stop'], row['lon_stop']), 
        axis=1)
    return stops_pdf.loc[distances.idxmin(), 'stop_id']


def haversine(lon1, lat1, lon2, lat2):
    
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return km


# raw_stg1_dispatch_dtsrc0 = glueContext.create_dynamic_frame_from_options(
#       connection_type="s3", 
#       connection_options = {
#         "paths": [args['s3_input_path_dispatch']],
#         "recurse":True,
#         "groupFiles":"inPartition"
#       }, 
#       format="parquet",
#       format_options={
#         "withHeader": True,
        
#     },
#     transformation_ctx="raw_stg1_dispatch_dtsrc0")

# raw_stg1_gps_raw_dtsrc0 = glueContext.create_dynamic_frame_from_options(
#       connection_type="s3", 
#       connection_options = {
#         "paths": [args['s3_input_path_gps']],
#         "recurse":True,
#         "groupFiles":"inPartition"
#       }, 
#       format="parquet",
#       format_options={
#         "withHeader": True,
        
#     },
#     transformation_ctx="raw_stg1_gps_raw_dtsrc0")

raw_stg1_dispatch_dtsrc0 = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "raw_stg1_dispatch", transformation_ctx = "raw_stg1_dispatch_dtsrc0")
raw_stg1_gps_raw_dtsrc0 = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "raw_stg1_gps_raw", transformation_ctx = "raw_stg1_gps_raw_dtsrc0")



raw_stg1_stops_dtsrc0 = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "raw_stg1_stop_info", transformation_ctx = "raw_stg1_stops_dtsrc0")


raw_stg1_dispatch_df = raw_stg1_dispatch_dtsrc0.toDF()
raw_stg1_gps_raw_df = raw_stg1_gps_raw_dtsrc0.toDF()
stops_df = raw_stg1_stops_dtsrc0.toDF()


raw_stg1_dispatch_df.createOrReplaceTempView("raw_dispatch")
raw_stg1_gps_raw_df.createOrReplaceTempView("raw_gps_raw")

dispatch_gps_df = spark.sql(
'''
SELECT 
d.sessionstarttime,
d.sessionendtime,
d.routeid,
g.longitude, g.latitude, g.timestamp, g.vehicle_no
FROM raw_dispatch d 
left join raw_gps_raw  g
on d.userid = g.vehicle_no
and g.timestamp between d.sessionstarttime and d.sessionendtime
''')

dispatch_gps_pdf = dispatch_gps_df.withColumn("latitude", dispatch_gps_df["latitude"].cast(DoubleType()))\
                                 .withColumn("longitude", dispatch_gps_df["longitude"].cast(DoubleType()))

stops_df = stops_df.withColumn("lat_stop", stops_df["lat_stop"].cast(DoubleType()))\
                                 .withColumn("lon_stop", stops_df["lon_stop"].cast(DoubleType()))


stops_pdf = stops_df.toPandas()



find_nearest_udf = udf(find_nearest, StringType())
dispatch_gps_pdf = dispatch_gps_pdf.withColumn("stop_id",find_nearest_udf(dispatch_gps_pdf.latitude,dispatch_gps_pdf.longitude))







dispatch_gps_pdf = dispatch_gps_pdf.join(stops_df,['stop_id'],"left")


haversine_udf = udf(haversine, FloatType())
dispatch_gps_pdf = dispatch_gps_pdf.withColumn("distance",haversine_udf(dispatch_gps_pdf.longitude,dispatch_gps_pdf.latitude,dispatch_gps_pdf.lon_stop,dispatch_gps_pdf.lat_stop))

dispatch_gps_pdf = dispatch_gps_pdf.withColumn("is_stop", \
   when((dispatch_gps_pdf.distance < 0.03), True).otherwise(False) \
  ).drop('distance','lat_stop','lon_stop','sessionendtime','sessionstarttime','latitude','longitude')

dispatch_gps_pdf = dispatch_gps_pdf.where(dispatch_gps_pdf.is_stop).drop('is_stop').withColumnRenamed("timestamp","timestamp_val")

dispatch_gps_pdf = dispatch_gps_pdf.withColumn("df_timestamp", from_unixtime((dispatch_gps_pdf.timestamp_val/1000)))\
            .withColumn("metric_hour", hour(col("df_timestamp")))\
            .withColumn("metric_month", month(col("df_timestamp")))\
            .withColumn("metric_day", dayofmonth(col("df_timestamp")))\
            .withColumn("metric_year", year(col("df_timestamp"))).drop("df_timestamp")



#dispatch_gps_pdf.write.mode("overwrite").parquet(s3_output_path_gps_dispatch_stop)
dispatch_gps_pdf.write.mode("overwrite").partitionBy("metric_year","metric_month","metric_day","metric_hour","routeid").parquet(s3_output_path_gps_dispatch_stop_time)

job.commit()







