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
CustomerCurated_node1703706276926 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://017747176708-udacity-project3-s3/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1703706276926",
)

# Script generated for node Amazon S3
AmazonS3_node1703706731522 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://017747176708-udacity-project3-s3/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1703706731522",
)

# Script generated for node Join
Join_node1703708342994 = Join.apply(
    frame1=CustomerCurated_node1703706276926,
    frame2=RenamedkeysforJoin_node1703708886703,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1703708342994",
)

# Script generated for node Change Schema
ChangeSchema_node1703708608629 = ApplyMapping.apply(
    frame=Join_node1703708342994,
        mappings=[
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="ChangeSchema_node1703708608629",
)

# Script generated for node Amazon S3
AmazonS3_node1703708667956 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1703708608629,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703708667956",
)
AmazonS3_node1703708667956.setCatalogInfo(
    catalogDatabase="stedidb", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1703708667956.setFormat("json")
AmazonS3_node1703708667956.writeFrame(Join_node1703708342994)

job.commit()
