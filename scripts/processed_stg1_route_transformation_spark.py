import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd

from pyspark.sql.types import DoubleType, StringType, FloatType
from pyspark.sql.functions import udf
from math import radians, cos, sin, asin, sqrt

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


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

s3_output_path_route_stop_seq = 's3://chalo-quess-ml-test/glue_processed/processed/tmp/route_stop_sequence'

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "raw_stg1_route_info", transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("ag", "string", "ag", "string"), ("di", "string", "di", "string"), ("f", "string", "f", "string"), ("il", "string", "il", "string"), ("iscomplete", "string", "iscomplete", "string"), ("l", "string", "l", "string"), ("lu", "string", "lu", "string"), ("name", "string", "name", "string"), ("poly", "string", "poly", "string"), ("rd", "string", "rd", "string"), ("rr", "string", "rr", "string"), ("seq", "string", "seq", "string"), ("sf", "string", "sf", "string"), ("sid", "string", "sid", "string"), ("so", "string", "so", "string"), ("unactive", "string", "unactive", "string"), ("o", "string", "o", "string"), ("distance", "string", "distance", "string"), ("isfreeride", "string", "isfreeride", "string"), ("islive", "string", "islive", "string"), ("mticketenabled", "string", "mticketenabled", "string"), ("routepassenabled", "string", "routepassenabled", "string"), ("polyindicesforstops", "string", "polyindicesforstops", "string"), ("route_id", "string", "route_id", "string")], transformation_ctx = "applymapping1")


raw_stg1_stops_dtsrc0 = glueContext.create_dynamic_frame.from_catalog(database = "chalo_processed", table_name = "raw_stg1_stop_info", transformation_ctx = "raw_stg1_stops_dtsrc0")


route_info_df = applymapping1.toDF()

stops_sdf = raw_stg1_stops_dtsrc0.toDF().drop("name")
route_info_pdf = route_info_df.toPandas()

selected_df = route_info_pdf[['route_id', 'seq']]



route_stop_seq_list = []
for index, row in selected_df.iterrows():
	seq_stops_list = row['seq'].split(' ')
	seq_counter = 0
	
	for source in seq_stops_list[0:len(seq_stops_list) - 1]:
		seq_counter = seq_counter + 1

		destination = seq_stops_list[seq_counter]
		
		new_json = {
			"route_id": row['route_id'],
			"source"  :source,
			"destination" :seq_stops_list[seq_counter],
			"seq_no": seq_counter
		}

		route_stop_seq_list.append(new_json)


route_stop_seq_pdf = pd.json_normalize(route_stop_seq_list)
route_seq_info_sdf=spark.createDataFrame(route_stop_seq_pdf) 



route_seq_info_v2_sdf = route_seq_info_sdf.join(stops_sdf, (route_seq_info_sdf.source == stops_sdf.stop_id),"left").withColumnRenamed("lat_stop","lat_stop_source").withColumnRenamed("lon_stop","lon_stop_source").drop("stop_id")
#route_seq_info_sdf.show(truncate = False)

route_seq_info_v2_sdf = route_seq_info_v2_sdf.join(stops_sdf, (route_seq_info_v2_sdf.destination == stops_sdf.stop_id),"left").withColumnRenamed("lat_stop","lat_stop_destination").withColumnRenamed("lon_stop","lon_stop_destination").drop("stop_id")

route_seq_info_v2_sdf = route_seq_info_v2_sdf \
.withColumn("lat_stop_source", route_seq_info_v2_sdf["lat_stop_source"].cast(DoubleType())) \
.withColumn("lon_stop_source", route_seq_info_v2_sdf["lon_stop_source"].cast(DoubleType())) \
.withColumn("lat_stop_destination", route_seq_info_v2_sdf["lat_stop_destination"].cast(DoubleType())) \
.withColumn("lon_stop_destination", route_seq_info_v2_sdf["lon_stop_destination"].cast(DoubleType()))


route_seq_info_v2_sdf = route_seq_info_v2_sdf.na.drop()


haversine_udf = udf(haversine, FloatType())
route_seq_info_v2_sdf = route_seq_info_v2_sdf.withColumn("distance",haversine_udf(route_seq_info_v2_sdf.lon_stop_source,route_seq_info_v2_sdf.lat_stop_source,route_seq_info_v2_sdf.lon_stop_destination,route_seq_info_v2_sdf.lat_stop_destination)).drop('lat_stop_source','lon_stop_source','lat_stop_destination','lon_stop_destination')

#route_seq_info_v2_sdf.show(100,truncate = False)

#route_stop_seq_pdf.show(truncate = False)

route_seq_info_v2_sdf.write.mode("overwrite").partitionBy("route_id").parquet(s3_output_path_route_stop_seq)
job.commit()




