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

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1703808197913 = glueContext.create_dynamic_frame.from_catalog(
    database="edwin",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1703808197913",
)

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1685352206564 = glueContext.create_dynamic_frame.from_catalog(
    database="edwin",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1685352206564",
)

# Script generated for node SQL Query
SqlQuery2514 = """
select * from step_trainer_trusted stt inner join accelerometer_trusted a_t on stt.sensorreadingtime = a_t.timestamp
"""
SQLQuery_node1703808216810 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2514,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1703808197913,
        "step_trainer_trusted": StepTrainerTrusted_node1685352206564,
    },
    transformation_ctx="SQLQuery_node1703808216810",
)

# Script generated for node MachineLearning
MachineLearning_node1703808234253 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1703808216810,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://edwin-lake-house/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearning_node1703808234253",
)

job.commit()
