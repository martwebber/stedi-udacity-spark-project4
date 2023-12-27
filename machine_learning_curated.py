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

# Script generated for node S3 bucket
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://017747176708-udacity-project3-s3/accelerometer/trusted/"],
        "compression": "gzip",
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Amazon S3
StepTrainerTrusted_node1692030411186 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://017747176708-udacity-project3-s3/step_trainer/trusted/"],
        "compression": "gzip",
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1692030411186",
)

# Script generated for node Join
Join_node1892324075479 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainerTrusted_node1692030411186,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1892324075479",
)

# Script generated for node Drop Fields
DropFields_node1692034429091 = DropFields.apply(
    frame=Join_node1892324075479,
    paths=["user"],
    transformation_ctx="DropFields_node1692034429091",
)

# Script generated for node S3 bucket
MachineLearningCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1692034429091,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://017747176708-udacity-project3-s3/machine_learning/curated/",
        "compression": "gzip",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node3",
)

job.commit()