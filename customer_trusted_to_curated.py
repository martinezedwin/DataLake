import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node CustomerTrusted
CustomerTrusted_node1685352206564 = glueContext.create_dynamic_frame.from_catalog(
    database="edwin",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1685352206564",
)

# Script generated for node AcclerometerLanding
AcclerometerLanding_node1703805259041 = glueContext.create_dynamic_frame.from_catalog(
    database="edwin",
    table_name="accelerometer_landing",
    transformation_ctx="AcclerometerLanding_node1703805259041",
)

# Script generated for node Join
Join_node1703805284647 = Join.apply(
    frame1=AcclerometerLanding_node1703805259041,
    frame2=CustomerTrusted_node1685352206564,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1703805284647",
)

# Script generated for node Drop Fields
DropFields_node1703805325665 = DropFields.apply(
    frame=Join_node1703805284647,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1703805325665",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1703805825169 = DynamicFrame.fromDF(
    DropFields_node1703805325665.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1703805825169",
)

# Script generated for node Amazon S3
AmazonS3_node1703805398512 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1703805825169,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://edwin-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1703805398512",
)

job.commit()
