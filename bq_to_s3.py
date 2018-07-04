# BigQuery to S3 Transfer function
# Author: Sigurdur Geirsson
# Last modified 2018/07/04

import sys
import os
import subprocess
from pyspark.sql import SparkSession
from datetime import datetime
from google.cloud import bigquery

# Packages required:
# org.apache.hadoop:hadoop-aws:2.7.3,
# com.amazonaws:aws-java-sdk:1.7.4,
# com.google.cloud.bigdataoss:bigquery-connector:0.12.1-hadoop2,
# com.databricks:spark-avro_2.11:4.0.0

# Example run:
# spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.3,com.amazonaws:aws-java-sdk:1.7.4,com.google.cloud.bigdataoss:bigquery-connector:0.12.1-hadoop2,com.databricks:spark-avro_2.11:4.0.0 \
# /home/hadoop/airflow/spark_jobs/bq_to_s3.py 20180601 dataset table s3a://bucket/target/

# Input Arguments
arg_date = sys.argv[1]
arg_dataset = sys.argv[2]
arg_table = sys.argv[3]
arg_s3directory = sys.argv[4]

# Get year, month and day
time = datetime.strptime(arg_date, '%Y%m%d')

# Initialize Spark
spark = SparkSession.builder.master("local").appName(f"BQ to S3 - {arg_dataset} {arg_table}").getOrCreate()

# Settings and authentication for AWS and Google
spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
#spark._jsc.hadoopConfiguration().set("fs.s3a.access.key",os.environ['aws_secret_access_key'])
#spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ['aws_access_key_id'])
spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark._jsc.hadoopConfiguration().set("fs.gs.project.id", "PROJECTID")	# <- Change needed(PROJECTID)
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/home/hadoop/setup_files/client_secret.json")

# BQ Conf
bigquery_client = bigquery.Client.from_service_account_json('/home/hadoop/setup_files/client_secret.json')
destination_uri = f'gs://PATH/bq_to_s3/{spark.sparkContext.applicationId}/pyspark_input-*.avro' # <- PATH Change
dataset_ref = bigquery_client.dataset(arg_dataset, project='PROJECTID')	# <- Change needed(PROJECTID)
table_ref = dataset_ref.table(arg_table)
job_config = bigquery.job.ExtractJobConfig()
#job_config.compression = 'SNAPPY' 		# Options: Snappy, GZIP
job_config.destination_format = 'AVRO'  # Options: AVRO, NEWLINE_DELIMITED_JSON

# BQ Table -> GCS
extract_job = bigquery_client.extract_table(table_ref, destination_uri, job_config=job_config)
extract_job.result()  # Waits for job to complete.

# GCS -> Spark DF
df = spark.read.format("com.databricks.spark.avro").load(destination_uri)

# SPARK DF -> S3
output_directory= arg_s3directory+f'{str(time.year).zfill(4)}/{str(time.month).zfill(2)}/{str(time.day).zfill(2)}'
df.write.parquet(output_directory) 

# GCS Cleanup
subprocess.call("gsutil rm -r gs://PATH/bq_to_s3/" + spark.sparkContext.applicationId, shell=True) # <- PATH Change
