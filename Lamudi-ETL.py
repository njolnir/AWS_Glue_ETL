import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Retrieve job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from AWS Glue Data Catalog and convert to DataFrame
data_df = glueContext.create_dynamic_frame.from_catalog(database="<YOUR_DATABASE>", table_name="<YOUR_TABLE>").toDF()

# Drop duplicates
data_df = data_df.dropDuplicates()

# Filter out records where tcp or floor_area is 0
data_df = data_df.filter((F.col('tcp') > 0) & (F.col('floor_area') > 0))

# Filter out records where 'name' consists only of numbers
data_df = data_df.filter(~F.col('name').rlike('^[0-9]+$'))

# Add Price_per_SQM column (example calculation)
data_df = data_df.withColumn('Price_per_SQM', F.col('tcp') / F.col('floor_area'))

# Coalesce the DataFrame to a single partition
data_df = data_df.coalesce(1)

# Convert DataFrame back to DynamicFrame
data_frame = DynamicFrame.fromDF(data_df, glueContext, "data_frame")

# Apply schema changes
data_frame = ApplyMapping.apply(
    frame=data_frame, 
    mappings=[
        ("name", "string", "name", "string"), 
        ("location", "string", "location", "string"), 
        ("tcp", "long", "tcp", "decimal"), 
        ("floor_area", "double", "floor_area", "bigint"), 
        ("bedrooms", "long", "bedrooms", "long"), 
        ("baths", "long", "baths", "long"), 
        ("club house", "long", "club house", "long"), 
        ("gym", "long", "gym", "long"), 
        ("swimming pool", "long", "swimming pool", "long"), 
        ("security", "long", "security", "long"), 
        ("cctv", "long", "cctv", "long"), 
        ("reception area", "long", "reception area", "long"), 
        ("parking area", "long", "parking area", "long"), 
        ("source", "string", "source", "string"), 
        ("city/town", "string", "city/town", "string"),
        ("Price_per_SQM", "double", "Price_per_SQM", "decimal")
    ]
)

# Write data to Amazon S3
glueContext.write_dynamic_frame.from_options(
    frame=data_frame, 
    connection_type="s3", 
    format="csv", 
    connection_options={"path": "<YOUR_S3_BUCKET>", "partitionKeys": []}
)

# Commit job
job.commit()
