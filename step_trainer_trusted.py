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

# Script generated for node Customer Curated
CustomerCurated_node1692890168057 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://udacity-data-engineering-course-s3-datalake/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1692890168057",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://udacity-data-engineering-course-s3-datalake/step_trainer/landing/"
        ],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Renamed keys for Filter Trusted Customers
RenamedkeysforFilterTrustedCustomers_node1692890283463 = ApplyMapping.apply(
    frame=CustomerCurated_node1692890168057,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforFilterTrustedCustomers_node1692890283463",
)

# Script generated for node Filter Trusted Customers
FilterTrustedCustomers_node1692890234921 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=RenamedkeysforFilterTrustedCustomers_node1692890283463,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="FilterTrustedCustomers_node1692890234921",
)

# Script generated for node Drop Extra Fields
DropExtraFields_node1692890319799 = DropFields.apply(
    frame=FilterTrustedCustomers_node1692890234921,
    paths=[
        "right_serialNumber",
        "right_birthDay",
        "right_customerName",
        "right_email",
        "right_phone",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "lastUpdateDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropExtraFields_node1692890319799",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1692890359430 = glueContext.getSink(
    path="s3://udacity-data-engineering-course-s3-datalake/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1692890359430",
)
StepTrainerTrusted_node1692890359430.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1692890359430.setFormat("json")
StepTrainerTrusted_node1692890359430.writeFrame(DropExtraFields_node1692890319799)
job.commit()

