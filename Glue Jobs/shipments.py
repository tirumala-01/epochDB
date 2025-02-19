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

table_name = "shipments"
s3_path = "s3://mydatasource-epochapp-demo/shipments-mini.json"
jdbc_url = "jdbc:postgresql://epochapp-rds-instance.cxofavsbhhbc.us-east-1.rds.amazonaws.com:5432/epochapp"
connection_properties = {
    "user": "",
    "password": "",
    "driver": "org.postgresql.Driver"
}

df = spark.read.option("multiline","true").json(s3_path)
df_transformed = df \
    .withColumnRenamed("origin", "shipment_origin") \
    .withColumnRenamed("destination", "shipment_destination") \
    .withColumn("shipment_weight", col("weight").cast("float")) \
    .withColumn("shipment_cost", col("cost").cast("float")) \
    .withColumn("shipment_delivery_time", col("delivery_time").cast("integer")) \
    .withColumn("shipment_log_id", regexp_replace(col("log_id"), "L-", "").cast("integer")) \
    .withColumn("shipment_full_log_id", col("log_id")) \
    .withColumn("cleaned_shipment_id", regexp_replace(col("shipment_id"), "S-", "").cast("integer")) \
    .withColumnRenamed("shipment_id", "shipment_full_id") \
    .withColumnRenamed("cleaned_shipment_id", "shipment_id") \
    .select("shipment_id", "shipment_full_id", "shipment_origin", "shipment_destination", "shipment_weight", "shipment_cost", "shipment_delivery_time", "shipment_log_id", "shipment_full_log_id")


df_transformed.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)
job.commit()