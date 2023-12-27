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

# Script generated for node Accerelometer Landing
AccerelometerLanding_node1703695255111 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://017747176708-udacity-project3-s3/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccerelometerLanding_node1703695255111",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1703695070249 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://017747176708-udacity-project3-s3/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1703695070249",
)

# Script generated for node Join
Join_node1703695410086 = Join.apply(
    frame1=AccerelometerLanding_node1703695255111,
    frame2=CustomerTrusted_node1703695070249,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1703695410086",
)

# Script generated for node Change Schema
ChangeSchema_node1703695561113 = ApplyMapping.apply(
    frame=Join_node1703695410086,
    mappings=[
        ("serialnumber", "string", "serialnumber", "string"),
        ("birthday", "string", "birthday", "string"),
        ("registrationdate", "bigint", "registrationdate", "bigint"),
        ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "bigint"),
        ("customername", "string", "customername", "string"),
        ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "bigint"),
        ("email", "string", "email", "string"),
        ("lastupdatedate", "bigint", "lastupdatedate", "bigint"),
        ("phone", "string", "phone", "string"),
        ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "bigint"),
    ],
    transformation_ctx="ChangeSchema_node1703695561113",
)

# Script generated for node Customer Curated
CustomerCurated_node1703695837412 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1703695561113,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://017747176708-udacity-project3-s3/customer/curated/",
        "compression": "gzip",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1703695837412",
)

job.commit()
