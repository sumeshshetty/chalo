import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


logger = glueContext.get_logger()





#for mandatory parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_input_path_dispatch','s3_input_path_gps_raw','s3_input_path_route_info','s3_input_path_stop_info'])
job.init(args['JOB_NAME'], args)



args['s3_input_path_dispatch']='s3://chalo-quess-ml-test/dispatch'
args['s3_input_path_gps_raw']='s3://chalo-quess-ml-test/tables/gps_raw'
args['s3_input_path_route_info']='s3://chalo-quess-ml-test/route_info'
args['s3_input_path_stop_info']='s3://chalo-quess-ml-test/stop_info'


s3_output_path_dispatch = 's3://chalo-quess-ml-test/glue_processed/raw/dispatch'
s3_output_path_gps_raw = 's3://chalo-quess-ml-test/glue_processed/raw/gps_raw'

s3_output_path_route_info = 's3://chalo-quess-ml-test/glue_processed/raw/route_info'
s3_output_path_stop_info = 's3://chalo-quess-ml-test/glue_processed/raw/stop_info'


datasource_dispatch = glueContext.create_dynamic_frame_from_options(
      connection_type="s3", 
      connection_options = {
        "paths": [args['s3_input_path_dispatch']]
      }, 
      format="csv",
      format_options={
        "withHeader": True,
        
    },
    transformation_ctx="datasource_dispatch")
   


applymapping1_dispatch = ApplyMapping.apply(frame = datasource_dispatch, mappings = [("`userid.keyword: descending`", "string", "userid", "string"), ("sessionstarttime: descending", "string", "sessionstarttime", "long"), ("sessionendtime: descending", "string", "sessionendtime", "long"), ("`routeid.keyword: descending`", "string", "routeid", "string"), ("count", "string", "count", "long")], transformation_ctx = "applymapping1_dispatch")
datasource_dispatch_df = applymapping1_dispatch.toDF()




datasource_gps_raw = glueContext.create_dynamic_frame_from_options(
      connection_type="s3", 
      connection_options = {
        "paths": [args['s3_input_path_gps_raw']]
      }, 
      format="csv", 
      format_options={
        "withHeader": True,
        
      },
      transformation_ctx="datasource_gps_raw")

datasource_gps_raw_df = datasource_gps_raw.toDF()



datasource_route_info = glueContext.create_dynamic_frame_from_options(
      connection_type="s3", 
      connection_options = {
        "paths": [args['s3_input_path_route_info']]
      }, 
      format="csv", 
      format_options={
        "withHeader": True,
    },
      transformation_ctx="datasource_route_info")


df_route_info = datasource_route_info.toDF()  
drop_columns = ('poly','polyindicesforstops')
df_route_info.drop(*drop_columns)


datasource_stop_info = glueContext.create_dynamic_frame_from_options(
      connection_type="s3", 
      connection_options = {
        "paths": [args['s3_input_path_stop_info']]
      }, 
      format="csv", 
      format_options={
        "withHeader": True,
    },
      transformation_ctx="datasource_stop_info")

applymapping1_stop_info = ApplyMapping.apply(frame = datasource_stop_info, mappings = [("lat", "string", "lat_stop", "double"), ("lon", "string", "lon_stop", "double"),("name", "string", "name", "string"),("stop_id", "string", "stop_id", "string")], transformation_ctx = "applymapping1_stop_info")
df_stops = applymapping1_stop_info.toDF()  



datasource_dispatch_df.write.mode("overwrite").partitionBy("userid").parquet(s3_output_path_dispatch)
datasource_gps_raw_df.write.mode("overwrite").partitionBy("vehicle_no").parquet(s3_output_path_gps_raw)

df_route_info.write.mode("overwrite").partitionBy("route_id").parquet(s3_output_path_route_info)
df_stops.write.mode("overwrite").partitionBy("stop_id").parquet(s3_output_path_stop_info)
job.commit()
      

      



