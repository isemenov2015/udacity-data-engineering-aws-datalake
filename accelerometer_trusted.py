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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://udacity-data-engineering-course-s3-datalake/accelerometer/landing/"
        ],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer trusted
Customertrusted_node1692878730087 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-data-engineering-course-s3-datalake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customertrusted_node1692878730087",
)

# Script generated for node Join Customer
JoinCustomer_node1692878696019 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=Customertrusted_node1692878730087,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1692878696019",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1692878896043 = glueContext.getSink(
    path="s3://udacity-data-engineering-course-s3-datalake/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1692878896043",
)
AccelerometerTrusted_node1692878896043.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1692878896043.setFormat("json")
AccelerometerTrusted_node1692878896043.writeFrame(JoinCustomer_node1692878696019)
job.commit()

