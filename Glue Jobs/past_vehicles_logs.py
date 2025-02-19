import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

table_name = "past_vehicles_logs"
s3_path = "s3://mydatasource-epochapp-demo/vehicles_logs-mini.json"
jdbc_url = "jdbc:postgresql://epochapp-rds-instance.cxofavsbhhbc.us-east-1.rds.amazonaws.com:5432/epochapp"
connection_properties = {
    "user": "",
    "password": "",
    "driver": "org.postgresql.Driver"
}

df = spark.read.option("multiline","true").json(s3_path)
df_transformed = df.withColumn("vl_id", regexp_replace(col("log_id"), "L-", "").cast("integer")) \
    .withColumn("vl_full_id", col("log_id")) \
    .withColumn("vl_vehicle_id", regexp_replace(col("vehicle_id"), "V-", "").cast("integer")) \
    .withColumn("vl_vehicle_full_id", col("vehicle_id")) \
    .withColumn("vl_trip_date", to_date(col("trip_date"), "yyyy-MM-dd")) \
    .filter(col("mileage").isNotNull() & col("fuel_used").isNotNull()) \
    .withColumn("vl_mileage", col("mileage").cast("float")) \
    .withColumn("vl_fuel_used", col("fuel_used").cast("float")) \
    .select("vl_id", "vl_full_id", "vl_vehicle_id", "vl_vehicle_full_id", "vl_trip_date", "vl_mileage", "vl_fuel_used")


df_transformed.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
job.commit()