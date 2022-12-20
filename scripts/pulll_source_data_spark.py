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



datasource_dispatch = glueContext.create_dynamic_frame_from_options(
      connection_type="s3", 
      connection_options = {
        "paths": [args['s3_input_path_dispatch']]
      }, 
      format="csv", 
      transformation_ctx="datasource_dispatch")
datasource_dispatch.show(5)
      
      
      
      