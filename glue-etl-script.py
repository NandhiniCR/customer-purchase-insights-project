import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Glue Data Catalog Table
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="purchase_data",
    table_name="data"
)

# Write directly to PostgreSQL RDS
datasink4 = glueContext.write_dynamic_frame.from_options(
    frame=datasource0,
    connection_type="postgresql",
    connection_options={
        "url": "jdbc:postgresql://my-postgre-db.czeak84a4jhd.us-east-2.rds.amazonaws.com:5432/postgres",
        "dbtable": "purchase",
        "user": "A123456",
        "password": "Mydatadb123"
    }
)

job.commit()