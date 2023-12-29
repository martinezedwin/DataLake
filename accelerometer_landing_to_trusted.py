import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1685352206564 = glueContext.create_dynamic_frame.from_catalog(
    database="edwin",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1685352206564",
)

# Script generated for node CustomerTrusted
CustomerTrusted_node1703801545043 = glueContext.create_dynamic_frame.from_catalog(
    database="edwin",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1703801545043",
)

# Script generated for node Join
Join_node1703803169794 = Join.apply(
    frame1=AccelerometerLanding_node1685352206564,
    frame2=CustomerTrusted_node1703801545043,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1703803169794",
)

# Script generated for node Drop Fields
DropFields_node1703803559677 = DropFields.apply(
    frame=Join_node1703803169794,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1703803559677",
)

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1703803634054 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703803559677,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://edwin-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1703803634054",
)

job.commit()
