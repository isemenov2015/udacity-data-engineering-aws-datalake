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

# Script generated for node Customers Trusted
CustomersTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-data-engineering-course-s3-datalake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomersTrusted_node1",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1692890930520 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://udacity-data-engineering-course-s3-datalake/step_trainer/landing/"
        ],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1692890930520",
)

# Script generated for node Drop Duplicate Serial Numbers
DropDuplicateSerialNumbers_node1692891038208 = DynamicFrame.fromDF(
    StepTrainerLanding_node1692890930520.toDF().dropDuplicates(["serialNumber"]),
    glueContext,
    "DropDuplicateSerialNumbers_node1692891038208",
)

# Script generated for node Renamed keys for Join Customer Trusted and Step Trainer
RenamedkeysforJoinCustomerTrustedandStepTrainer_node1692891129449 = ApplyMapping.apply(
    frame=DropDuplicateSerialNumbers_node1692891038208,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "long"),
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("distanceFromObject", "int", "right_distanceFromObject", "int"),
    ],
    transformation_ctx="RenamedkeysforJoinCustomerTrustedandStepTrainer_node1692891129449",
)

# Script generated for node Join Customer Trusted and Step Trainer
JoinCustomerTrustedandStepTrainer_node1692891067408 = Join.apply(
    frame1=CustomersTrusted_node1,
    frame2=RenamedkeysforJoinCustomerTrustedandStepTrainer_node1692891129449,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="JoinCustomerTrustedandStepTrainer_node1692891067408",
)

# Script generated for node Drop Extra Fields
DropExtraFields_node1692891157550 = DropFields.apply(
    frame=JoinCustomerTrustedandStepTrainer_node1692891067408,
    paths=["right_serialNumber", "right_distanceFromObject", "sensorReadingTime"],
    transformation_ctx="DropExtraFields_node1692891157550",
)

# Script generated for node Customer Curated
CustomerCurated_node1692891169134 = glueContext.getSink(
    path="s3://udacity-data-engineering-course-s3-datalake/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1692891169134",
)
CustomerCurated_node1692891169134.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customers_curated"
)
CustomerCurated_node1692891169134.setFormat("json")
CustomerCurated_node1692891169134.writeFrame(DropExtraFields_node1692891157550)
job.commit()

