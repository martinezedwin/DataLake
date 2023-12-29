import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1685352206564 = glueContext.create_dynamic_frame.from_catalog(
    database="edwin",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1685352206564",
)

# Script generated for node CustomerCurated
CustomerCurated_node1703806562744 = glueContext.create_dynamic_frame.from_catalog(
    database="edwin",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1703806562744",
)

# Script generated for node SQL Query
SqlQuery2774 = """
select stl.* from customer_curated cc inner join step_trainer_landing stl on cc.serialnumber = stl.serialnumber
"""
SQLQuery_node1703807726965 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2774,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node1685352206564,
        "customer_curated": CustomerCurated_node1703806562744,
    },
    transformation_ctx="SQLQuery_node1703807726965",
)

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1703806769986 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1703807726965,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://edwin-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1703806769986",
)

job.commit()
