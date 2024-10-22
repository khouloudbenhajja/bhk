import sys
from exercice.config.config import app_config

try:
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import SparkConf
    from pyspark.context import SparkContext

    # @params: [JOB_NAME, "ENTRYPOINT", "ENV"]
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENTRYPOINT", "ENV"])

    role_arn_datalake = app_config.role_arn_datalake

    # access credentials
    sts_client = boto3.client("sts")
    assume_role_object = sts_client.assume_role(
        RoleArn=role_arn_datalake,
        RoleSessionName="AssumeRole-Session-Name-Whatever",
        DurationSeconds=3600,
    )
    credentials = assume_role_object["Credentials"]

    # Access Datalake objects using PySpark
    # conf spark
    conf = (
        SparkConf()
        .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .set("spark.hadoop.fs.s3a.access.key", credentials["AccessKeyId"])
        .set("spark.hadoop.fs.s3a.secret.key", credentials["SecretAccessKey"])
        .set("spark.hadoop.fs.s3a.session.token", credentials["SessionToken"])
        .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    )
    sc = SparkContext(conf=conf)
    sc.setLogLevel("TRACE")
    glueContext = GlueContext(sc)
    logger = glueContext.get_logger()
    spark = glueContext.spark_session
    job = Job(glueContext)

except ImportError:
    import logging
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("first exercice").getOrCreate()
    logging.basicConfig(
        format="%(asctime)s\t%(module)s\t%(levelname)s\t%(message)s", level=logging.INFO
    )
    logger = logging.getLogger(__name__)
    logger.warning("Package awsglue not found! Excepted if you run the code locally")
