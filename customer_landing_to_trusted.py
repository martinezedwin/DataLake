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

# Script generated for node CustomerLanding
CustomerLanding_node1685352206564 = glueContext.create_dynamic_frame.from_catalog(
    database="edwin",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1685352206564",
)

# Script generated for node SQL Query
SqlQuery2272 = """
select * from myDataSource where shareWithResearchAsOfDate > 0

"""
SQLQuery_node1703800950572 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2272,
    mapping={"myDataSource": CustomerLanding_node1685352206564},
    transformation_ctx="SQLQuery_node1703800950572",
)

# Script generated for node CostumerTrusted
CostumerTrusted_node1703800980227 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1703800950572,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://edwin-lake-house/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CostumerTrusted_node1703800980227",
)

job.commit()
