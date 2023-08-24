import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-data-engineering-course-s3-datalake/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLandingZone_node1",
)

# Script generated for node Share with Research
SharewithResearch_node1692732223404 = Filter.apply(
    frame=CustomerLandingZone_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="SharewithResearch_node1692732223404",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1692732392158 = glueContext.getSink(
    path="s3://udacity-data-engineering-course-s3-datalake/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerZone_node1692732392158",
)
TrustedCustomerZone_node1692732392158.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
TrustedCustomerZone_node1692732392158.setFormat("json")
TrustedCustomerZone_node1692732392158.writeFrame(SharewithResearch_node1692732223404)
job.commit()

