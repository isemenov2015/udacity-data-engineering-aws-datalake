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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1692895025016 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://udacity-data-engineering-course-s3-datalake/step_trainer/trusted/"
        ],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1692895025016",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1692895240662 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://udacity-data-engineering-course-s3-datalake/accelerometer/trusted/"
        ],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1692895240662",
)

# Script generated for node Join Accelerometer Trusted with Step Trainer Trusted data
JoinAccelerometerTrustedwithStepTrainerTrusteddata_node1692895524951 = Join.apply(
    frame1=AccelerometerTrusted_node1692895240662,
    frame2=StepTrainerTrusted_node1692895025016,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="JoinAccelerometerTrustedwithStepTrainerTrusteddata_node1692895524951",
)

# Script generated for node remove extra .SerialNumber and Anonymize by Drop Personal Info Fields
removeextraSerialNumberandAnonymizebyDropPersonalInfoFields_node1692895592523 = DropFields.apply(
    frame=JoinAccelerometerTrustedwithStepTrainerTrusteddata_node1692895524951,
    paths=["`.serialNumber`", "customerName", "email", "phone", "birthDay"],
    transformation_ctx="removeextraSerialNumberandAnonymizebyDropPersonalInfoFields_node1692895592523",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1692896588285 = glueContext.getSink(
    path="s3://udacity-data-engineering-course-s3-datalake/machine-learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1692896588285",
)
MachineLearningCurated_node1692896588285.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1692896588285.setFormat("json")
MachineLearningCurated_node1692896588285.writeFrame(
    removeextraSerialNumberandAnonymizebyDropPersonalInfoFields_node1692895592523
)
job.commit()

