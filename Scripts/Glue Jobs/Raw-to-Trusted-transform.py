import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

db_name = "lambda-poc"
tbl_name = "20017raw_lambda_poc"
s3_write_bucket = "s3://shift-to-trusted/"

#Extract 
# creating datasource using the catalog table
# crawler has added metadata to glue catalog from the datasets in the raw bucket 
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=db_name, table_name=tbl_name)

# converting from Glue DynamicFrame to Spark Dataframe
dataframe = datasource.toDF()

#Transform 
#dropping the column since it has no data and was from a source without cleaned data 
datasource_df = dataframe.drop("unnamed: 27")
datasource_df = datasource_df.drop("cancellation_code") #Has about 99% empty values and may not contribute effectively to a use case 

transform1 = DynamicFrame.fromDF(
    datasource_df, glueContext, 'transform1')

#Load 
# Uploading data back to the trusted s3 bucket after dropping columns irrelevant to analysis 
# further refinement of data will occur when data is being sent to the refined bucket 
datasink1 = glueContext.write_dynamic_frame.from_options(frame=transform1, connection_type="s3", connection_options={
                                                         "path": s3_write_bucket}, format="csv", transformation_ctx="datasink1")
                                                         

job.commit()