# S3 to BigQuery Transfer function
# Author: Sigurdur Geirsson
# Last modified 2018/07/04

import sys
import os
import subprocess
from pyspark.sql import SparkSession
from datetime import datetime

# Packages required:
# org.apache.hadoop:hadoop-aws:2.7.3,
# com.amazonaws:aws-java-sdk:1.7.4,
# com.google.cloud.bigdataoss:bigquery-connector:0.12.1-hadoop2,

# Example run:
# spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.3,com.amazonaws:aws-java-sdk:1.7.4,com.google.cloud.bigdataoss:bigquery-connector:0.12.1-hadoop2 \
# /home/hadoop/airflow/spark_jobs/s3_to_bq.py 20180601 s3a://bucket/source/ dataset table
# Note: source has s3a://bucket/source/year/month/day/ structure

# Input Arguments
arg_date = sys.argv[1]
arg_s3directory = sys.argv[2]
arg_dataset = sys.argv[3]
arg_table = sys.argv[4]

# Get year, month and day
time = datetime.strptime(arg_date, '%Y%m%d')
tyear = str(time.year).zfill(4)
tmonth = str(time.month).zfill(2)
tday = str(time.day).zfill(2)

# Initialize Spark
spark = SparkSession.builder.appName(f"S3 to BQ - {arg_dataset} {arg_table}").getOrCreate()

# Settings and authentication for AWS and Google
spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
#spark._jsc.hadoopConfiguration().set("fs.s3a.access.key",os.environ['aws_secret_access_key'])
#spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ['aws_access_key_id'])
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/home/hadoop/setup_files/client_secret.json")
spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark._jsc.hadoopConfiguration().set("fs.gs.project.id", "PROJECTID")	# <- Change needed(PROJECTID)
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")


# S3 -> Spark DF
df = spark.read.parquet(arg_s3directory+f'{tyear}/{tmonth}/{tday}/')

# Spark DF -> GCS
output_directory = f'gs://PATH/s3_to_bq/{spark.sparkContext.applicationId}/pyspark_output'	# <- PATH change
output_files = output_directory + '/part-*'
sql_context = df.write.parquet(output_directory)

# GCS - > BQ table
output_dataset = arg_dataset
output_table = arg_table

subprocess.check_call(
    'bq load --source_format=PARQUET '
    '--replace '
    '--autodetect '
    '{dataset}.{table} {files}'.format(
        dataset=output_dataset, table=output_table, files=output_files
    ).split())

# GCS Cleanup
subprocess.call("gsutil rm -r gs://PATH/s3_to_bq/" + spark.sparkContext.applicationId, shell=True)	# <- PATH change




