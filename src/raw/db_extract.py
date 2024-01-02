import json
import os
import sys
import datetime
import boto3
import base64
from botocore.exceptions import ClientError
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from src.common.db import read_from_db
from src.common.utils import get_secrets


sc = SparkContext()
glue_context = GlueContext(sc)
logger = glue_context.get_logger()


            

def process_table(
    glue_context,
    table_name,
    connection_options,
    bucket_name,
    update_cycle="2023_H1",
    region="Europe",
    db_name=None,
    job_run_id="1",
):
    """
    Process a table: read the data from SQL Server and write it to S3.
    :param glue_context: GlueContext object
    :param table_name: Name of the table to process
    :param connection_options: Connection options dictionary for the JDBC
    :param bucket_name: Name of the S3 bucket
    :param update_cycle: Update cycle parameter
    :param region: AWS region
    :param db_name: Database name
    :param job_run_id: Job run ID
    :return: None
    """
    if db_name:
        datasource0 = glue_context.create_dynamic_frame.from_options(
            connection_type="sqlserver",
            connection_options=connection_options
        )

        glue_context.write_dynamic_frame.from_options(
            frame=datasource0,
            connection_type="s3",
            connection_options={
                "path": f"s3://{bucket_name}/{update_cycle}/{region}/raw/{db_name}/{job_run_id}/{table_name}"
            },
            format="parquet"
        )

def main():
    """
    Main function: entry point of the script.
    :return: None
    """
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "REGION",
            "BUCKET_NAME",
            "UPDATE_CYCLE",
            "DB_NAME",
            "TABLE_NAMES",
            "SECRET_NAME"
        ],
    )

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    secret_data = get_secrets(args["SECRET_NAME"], args["REGION"])

    table_list = args["TABLE_NAMES"].split(",")

    for table_name in table_list:
        connection_options = {
            "url": f"jdbc:sqlserver://{secret_data['host']}:1433/{args['DB_NAME']}",
            "dbtable": table_name,
            "user": secret_data["db_user"],
            "password": secret_data["db_password"],
        }

        process_table(
            glue_context=glue_context,
            table_name=table_name,
            connection_options=connection_options,
            bucket_name=args["BUCKET_NAME"],
            db_name=args["DB_NAME"],
            job_run_id=1,
        )

    job.commit()


if __name__ == "__main__":
    main()