import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from math import radians, cos, sin, asin, sqrt

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_output_path_gps_dispatch_stop = 's3://chalo-quess-ml-test/glue_processed/processed/gps_dispatch_stop_info'



def dist(lat1, long1, lat2, long2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
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
    distances = raw_stg1_stops_pdf.apply(
        lambda row: dist(lat, long, row['lat_stop'], row['lon_stop']), 
        axis=1)
    return raw_stg1_stops_pdf.loc[distances.idxmin(), 'stop_id']


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return km

raw_stg1_dispatch_dtsrc0 = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "raw_stg1_dispatch", transformation_ctx = "raw_stg1_dispatch_dtsrc0")
raw_stg1_gps_raw_dtsrc0 = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "raw_stg1_gps_raw", transformation_ctx = "raw_stg1_gps_raw_dtsrc0")

#raw_stg1_route_dtsrc0 = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "raw_stg1_route_info", transformation_ctx = "raw_stg1_route_dtsrc0")
raw_stg1_stops_dtsrc0 = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "raw_stg1_stop_info", transformation_ctx = "raw_stg1_stops_dtsrc0")


raw_stg1_dispatch_df = raw_stg1_dispatch_dtsrc0.toDF()
raw_stg1_gps_raw_df = raw_stg1_gps_raw_dtsrc0.toDF()

#raw_stg1_route_df = raw_stg1_route_dtsrc0.toDF()
raw_stg1_stops_df = raw_stg1_stops_dtsrc0.toDF()

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



dispatch_gps_pdf = dispatch_gps_df.toPandas()



dispatch_gps_pdf["latitude"] = pd.to_numeric(dispatch_gps_pdf["latitude"])
dispatch_gps_pdf["longitude"] = pd.to_numeric(dispatch_gps_pdf["longitude"])


#raw_stg1_route_pdf = raw_stg1_route_df.toPandas()
raw_stg1_stops_pdf = raw_stg1_stops_df.toPandas()


dispatch_gps_pdf['stop_id'] = dispatch_gps_pdf.apply(
    lambda row: find_nearest(row['latitude'], row['longitude']), 
    axis=1)



dispatch_gps_pdf = pd.merge(dispatch_gps_pdf,raw_stg1_stops_pdf[['stop_id','name','lat_stop','lon_stop']],on='stop_id', how='left')


dispatch_gps_pdf['distance'] = [haversine(dispatch_gps_pdf.longitude[i],dispatch_gps_pdf.latitude[i],dispatch_gps_pdf.lon_stop[i],dispatch_gps_pdf.lat_stop[i]) for i in range(len(dispatch_gps_pdf))]
dispatch_gps_pdf['distance'] = dispatch_gps_pdf['distance'].round(decimals=3)



dispatch_gps_spark_df=spark.createDataFrame(dispatch_gps_pdf) 



dispatch_gps_spark_df.write.mode("overwrite").partitionBy("routeid").parquet(s3_output_path_gps_dispatch_stop)
job.commit()
