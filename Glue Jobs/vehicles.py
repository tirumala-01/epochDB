import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import regexp_replace, col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define S3 path and RDS connection options
s3_path = "s3://mydatasource-epochapp-demo/vehicles.json"
rds_connection_options = {
    "url": "jdbc:postgresql://epochapp-rds-instance.cxofavsbhhbc.us-east-1.rds.amazonaws.com:5432/epochapp",
    "dbtable": "vehicles",
    "user": "",
    "password": "",
    "driver": "org.postgresql.Driver",
}

# Read JSON from S3
s3_df = spark.read.option("multiline","true").json(s3_path)

# Transform: Remove 'V-' prefix from vehicle_id and convert to integer
s3_df_transformed = s3_df.withColumn("vehicle_full_id", col("vehicle_id")) \
    .withColumn("vehicle_id", regexp_replace(col("vehicle_id"), "V-", "").cast("int")) \
    .withColumnRenamed("name", "vehicle_name") \
    .withColumnRenamed("total_mileage", "vehicle_total_mileage")



# Convert DataFrame to Glue DynamicFrame
dy_frame = DynamicFrame.fromDF(s3_df_transformed, glueContext, "dyf")

# Write data to RDS PostgreSQL
glueContext.write_dynamic_frame.from_options(
    frame=dy_frame, connection_type="jdbc", connection_options=rds_connection_options
)

job.commit()