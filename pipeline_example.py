# Pipeline Example
# Author: Sigurdur Geirsson
# Last modified 2018/07/04

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators import SparkSubmitOperator
from airflow.operators import BashOperator

packages = "--packages org.apache.hadoop:hadoop-aws:2.7.3,com.amazonaws:aws-java-sdk:1.7.4,com.google.cloud.bigdataoss:bigquery-connector:0.12.1-hadoop2,com.databricks:spark-avro_2.11:4.0.0"

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2018, 6, 1),
    'end_date': datetime(2018, 6, 2),
    'email': ['user@email.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 10,
}

schedule_interval = "0 1 * * *"
dag = DAG('pipeline_example', default_args=default_args, schedule_interval=schedule_interval)

source_location = 's3a://BUCKET/PATH/rawdata/' # <- Path Change
target_location = 's3a://BUCKET/PATH/events/' # <- Path Change

t1 = SparkSubmitOperator(
    task_id='preprocess',
    application_file="/home/hadoop/airflow/spark_jobs/preprocessing.py",
    other_spark_options=packages,
    application_args="{{ ds_nodash }} " + source_location + " " + target_location,
    dag=dag
)

t2 = BigQueryOperator(
    task_id='create_table',
    use_legacy_sql=True,
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    bql='''
    #legacySQL
    select country from events.user_info_{{ ds_nodash }}
    group by 1
    order by 1
    ''',
    destination_dataset_table='test.user_info_test_{{ ds_nodash }}',
    dag=dag)

t3 = SparkSubmitOperator(
    task_id='bq_to_s3',
    application_file="/home/hadoop/airflow/spark_jobs/bq_to_s3.py",
    other_spark_options=packages,
    application_args="{{ ds_nodash }} test user_info_test_{{ ds_nodash }} s3a://BUCKET/PATH/user_info_test/",   # <- Path Change
    dag=dag
)

t4 = SparkSubmitOperator(
    task_id='s3_to_bq',
    application_file="/home/hadoop/airflow/spark_jobs/s3_to_bq.py",
    other_spark_options=packages,
    application_args="{{ ds_nodash }} " + "s3a://BUCKET/PATH/user_info_test/" + " test user_info_test_froms3_{{ ds_nodash }}",  # <- Path Change
    dag=dag
)

# Set task dependencies

t1 

t2 >> t3 >> t4 




















