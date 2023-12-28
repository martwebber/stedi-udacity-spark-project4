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

# Script generated for node CustomerTrusted
CustomerTrusted_node1703690101072 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://017747176708-udacity-project3-s3/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1703690101072",
)

# Script generated for node Amazon S3
AmazonS3_node1703693258616 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://017747176708-udacity-project3-s3/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1703693258616",
)

# Script generated for node Join
Join_node1703693301529 = Join.apply(
    frame1=AmazonS3_node1703693258616,
    frame2=CustomerTrusted_node1703690101072,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1703693301529",
)

# Script generated for node Change Schema
ChangeSchema_node1703704691854 = ApplyMapping.apply(
    frame=Join_node1703693301529,
    mappings=[
        ("user", "string", "user", "string"),
        ("timestamp", "bigint", "timestamp", "long"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
    ],
    transformation_ctx="ChangeSchema_node1703704691854",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1703704857211 = glueContext.getSink(
    path="s3://017747176708-udacity-project3-s3/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1703704857211",
)
AccelerometerTrusted_node1703704857211.setCatalogInfo(
    catalogDatabase="stedidb", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1703704857211.setFormat("json")
AccelerometerTrusted_node1703704857211.writeFrame(ChangeSchema_node1703704691854)
job.commit()
