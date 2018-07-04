# Preprocessing (filter, transform and sort data from a mobile application)
# Author: Sigurdur Geirsson
# Last modified 2018/07/04

# Packages required:
# org.apache.hadoop:hadoop-aws:2.7.3,
# com.amazonaws:aws-java-sdk:1.7.4,

# Example run: 
# spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.3,com.amazonaws:aws-java-sdk:1.7.4 /home/hadoop/airflow/spark_jobs/preprocessing.py 20180601 \
# s3a://bucketname/rawdata/ s3a://bucketname/processed_data/
# 

from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType,ArrayType, LongType
import re
import os
import time
import sys
import subprocess
from datetime import datetime

# Initialize Spark
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark = (SparkSession
          .builder
          .appName("Preprocessing")
          .config("spark.driver.memory", "8g")
          .config("spark.executor.memory","2g")
          .config("spark.sql.caseSensitive","true")
          .config("spark.sql.parquet.mergeSchema","false")
          .config("spark.hadoop.parquet.enable.summary-metadata", "false")
          .getOrCreate())

# Settings and authentication for AWS and Google
spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
#spark._jsc.hadoopConfiguration().set("fs.s3a.access.key",os.environ['aws_secret_access_key'])
#spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ['aws_access_key_id']) 
spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark._jsc.hadoopConfiguration().set("fs.gs.project.id", "PROJECTID")  # <- Change needed(PROJECTID)                                                          
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", "/home/hadoop/setup_files/client_secret.json")   
spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark._jsc.hadoopConfiguration().set("spark.speculation", "false")
spark._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")

# Logger to output to Spark Console
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

def flatten(schema, prefix=None):
    # Source: https://stackoverflow.com/questions/37471346/automatically-and-elegantly-flatten-dataframe-in-spark-sql
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)
    return fields

def preprocess_1(source_location, json_location):

    # Load all data into Spark DF, I group the files for memory efficiency before importing it into hdfs
    source_dir_only = re.sub('s3a://','', source_location)
    subprocess.check_call("s3-dist-cp --src=" + source_location + " --dest=hdfs:///" + source_dir_only + " --groupBy='.*/(\w+)/.*' --targetSize=10", shell=True)

    raw_data = spark.read.json("hdfs:///" + f'{source_dir_only}/*')

    # Get list of all event_types and exclude those in the excluded_event_types list
    raw_data.createOrReplaceTempView("Raw_Data_Table")
    event_types = spark.sql("SELECT event_type from Raw_Data_Table group by 1 order by 1 desc")
    event_types_list = event_types.rdd.map(lambda r: r[0]).collect()
    excluded_event_types = ["Event1","Event2","Event3"]
    event_types = [x for x in event_types_list if x not in excluded_event_types]
    event_types = [event_types[i] for i, x in enumerate(event_types) if not re.search(r'[\_,\s]+', x)]

    # Filter out columns
    raw_data = raw_data.drop('awsAccountId')
    raw_data = raw_data.drop('client_context')

    # Flatten DF and filter out columns with capital letters and spaces 
    flatten_raw_schema = flatten(raw_data.schema)
    flatten_filtered_schema = [flatten_raw_schema[i] for i, x in enumerate(flatten_raw_schema) if not re.search(r'[A-Z\s]+', x)]

    # Create a recipe for a new flattened DF
    string = ""
    for i in list(range(0, len(flatten_filtered_schema))):
        filtered = re.sub('application.package_name','package_name',flatten_filtered_schema[i])
        filtered = re.sub('application.title','app_title',filtered)
        filtered = re.sub('application.version_code','app_version_code',filtered)
        filtered = re.sub('application.app_id','app_id',filtered)
        filtered = re.sub('application.cognito_identity_pool_id','cognito_identity_pool_id',filtered)
        filtered = re.sub('application.version_name','app_version_name',filtered)
        filtered = re.sub('application.sdk.version','sdk_version',filtered)
        filtered = re.sub('application.sdk.name','sdk_name',filtered)
        filtered = re.sub('session.session_id','session_id',filtered)
        filtered = re.sub('device.locale.country','country',filtered)
        filtered = re.sub('device.locale.code"','locale',filtered)
        filtered = re.sub('device.locale.language','language',filtered)
        filtered = re.sub('device.platform.version','platform_version',filtered)
        filtered = re.sub('device.platform.name','platform',filtered)
        filtered = re.sub('device.make','make',filtered)
        filtered = re.sub('device.model','model',filtered)
        filtered = re.sub('client.cognito_id','cognito_id',filtered)
        filtered = re.sub('client.client_id','client_id',filtered)
        filtered = re.sub('attributes.user_id','user_id',filtered)
        filtered = re.sub('[^A-Za-z0-9,.,_]+', '', filtered)
        filtered = re.sub('\.', '_', filtered)
        string = string + "F.col('" + flatten_filtered_schema[i] + "').alias('" + filtered + "'),"
    string = string[:-1]
    exec("flatten_df = raw_data.select(" + string + ")", locals(), globals())
    
    # Partition DF by Event_Type column and output it to json_location
    flatten_df2 = flatten_df.withColumn('et', flatten_df.event_type)
    flatten_df3 = flatten_df2.where(flatten_df2.event_type.isin(event_types))
    flatten_df3.write.option("compression", "gzip").partitionBy("et").json(json_location)
    LOGGER.warn('Finished the JSON event split')

def preprocess_2(json_location, schema_location, output_location, hour):

    # Get list of all directories/files in hdfs directory
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(json_location[:-1]))
    result = [file.getPath().getName() for file in list_status]
    
    # Remove the SUCCESS hadoop file from the list frrom above
    result = [result[i] for i, x in enumerate(result) if not re.search(r'_SUCCESS', x)] 
    result = sorted(result)

    # Get paths to JSON events
    json_events = []
    for i in range(0,len(result)):                  
        json_events.append(json_location[:-1] + result[i])


    for i in range(0,len(json_events)):
        df_event = sqlContext.read.json(json_events[i] + '/*')

        if (not df_event.rdd.isEmpty()):

            # Get event_type being processed 
            json_string = re.sub(json_location + 'et=', '', json_events[i])
            json_string = re.sub('/','',json_string)

            # Print out Event type being processed to Spark Console
            LOGGER.warn('Event type: ' + json_string)

            # Figure out newest version of app for that particular event_type
            df_event.createOrReplaceTempView("Event_Type_Table")
            df_event_sql = spark.sql("SELECT app_version_name,count(*) from Event_Type_Table group by 1 order by 1 desc limit 100")
            df_event_sql_version_filter = df_event_sql.filter(df_event_sql["app_version_name"].rlike("\d+.\d+.\d+"))

            if (df_event_sql_version_filter.rdd.isEmpty()):
                LOGGER.warn('Event type RDD is empty: ' + json_string)

            if (not df_event_sql_version_filter.rdd.isEmpty()):
                newest_version = max(df_event_sql_version_filter.select('app_version_name').distinct().rdd.map(lambda r: r[0]).take(10))

                # Infer Schema (Based on JSON sample from the newest version of the app)
                string = f'SELECT * from Event_Type_Table where app_version_name=="{newest_version}" limit 100'
                sample = spark.sql(string)
                sample = sample.sample(False, 1.0).limit(10)
                sample.write.json(schema_location + json_string + '/' + hour)
                schemer = spark.read.json(schema_location + json_string + '/*')
                schema = schemer.schema

                # Read JSON data based on Schema and save DF as Parquet
                df_event_final = spark.read.json(json_events[i] + '/*', schema)
                df_event_final.repartition(1).write.option("maxRecordsPerFile", 100000).parquet(output_location + json_string + '/' + hour)

def preprocess_everything(time, source, target):

    # Find year, month, day for target paths
    time = datetime.strptime(time, '%Y%m%d')
    tyear = str(time.year).zfill(4)
    tmonth = str(time.month).zfill(2)
    tday = str(time.day).zfill(2)

    # Get s3a path without URI prefix
    source_dir_only = re.sub('s3a://','', source)
    source_dir_only= source_dir_only

    source_location = f'{source}/{tyear}/{tmonth}/{tday}/'
    json_location = f'hdfs:///{source_dir_only}_json/{tyear}/{tmonth}/{tday}/'
    schema_location = f'hdfs:///{source_dir_only}_schema/{tyear}/{tmonth}/{tday}/'
    parquet_location= f'hdfs:///{source_dir_only}_parquet/{tyear}/{tmonth}/{tday}/'

    # Cleanup (if previous run failed or was interrupted)
    os.system("hdfs dfs -rm -r -skipTrash " + f'hdfs:/{source_dir_only}')

    hours_to_run = list(range(0,24))
    hours_to_run = [0,1]
    for i in hours_to_run:
        hour=str(i).zfill(2)
        preprocess_1(f'{source_location}{hour}/',f'{json_location}{hour}/')
        preprocess_2(f'{json_location}{hour}/*',f'{schema_location}/', f'{parquet_location}/', hour)
        # Clean hdfs files after each run
        os.system("hdfs dfs -rm -r -skipTrash " + f'hdfs:/{source_dir_only}{tyear}/')
        os.system("hdfs dfs -rm -r -skipTrash " + f'hdfs:/{source_dir_only}_schema/')
        os.system("hdfs dfs -rm -r -skipTrash " + f'hdfs:/{source_dir_only}_json/')
    # Write to S3, after it has processed all 24 hours. This is a safety feature so it won't output unless everything was successful.
    subprocess.check_call(f's3-dist-cp --src={parquet_location} --dest={target}{tyear}/{tmonth}/{tday}/', shell=True)

    # Cleanup
    os.system("hdfs dfs -rm -r -skipTrash " + f'hdfs:/{source_dir_only}')

# Input Arguments
time = sys.argv[1]
source = sys.argv[2]
target = sys.argv[3]

preprocess_everything(time, source, target)
