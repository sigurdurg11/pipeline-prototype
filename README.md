# pipeline-prototype

Pipeline prototype 
------------------------------------

Batch processing pipeline running on Amazon EMR. 

It has 3 Apache Spark functions so far.  <br />
  1) Preprocess (filter, transform and sort) raw data from a mobile application  <br />
  2) Transfer data from S3 to BigQuery  <br />
  3) Transfer data from BigQuery to S3  <br />

I use Apache Airflow to manage tasks and their dependencies. It is easy to build pipelines in Airflow in a modular way. <br />

List of files
------------------------------------

`bootstrap_cluster.sh`                              Setup everything needed to run the pipeline on EMR <br />
`preprocessing_s3.py`                               Preprocessing - Apache Spark function <br />
`bq_to_s3.py`                                       BigQuery to S3 - Apache Spark function  <br />
`s3_to_bq.py`                                       S3 to BigQuery - Apache Spark function <br />

`pipeline_example.py`                               Simple Airflow DAG describing tasks and their dependencies <br />
 
`google-cloud-sdk-204.0.0-linux-x86_64.tar.gz`      Setup Google Cloud SDK <br />
`guava-24.1-jre.jar`                                Use newer version of Guava, needed for Google access <br />


Instructions
------------------------------------

1) Starting EMR cluster

Note: Need to change SUBNETID, PATHS <br />
`aws emr create-cluster --name "Pipeline Prototype" --tags Name="pipeline" \
--release-label emr-5.13.0 \
--log-uri s3://BUCKET/PATH/cluster/logs \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=r4.xlarge \
 InstanceGroupType=CORE,InstanceCount=2,InstanceType=r4.xlarge \
 --ec2-attributes KeyName=pipeline,SubnetId=subnet-SUBNETID \
 --use-default-roles \
 --applications Name=Hive Name=Spark \
 --bootstrap-actions \
 Path=Name="wj bootstrap",Path="s3://BUCKET/PATH/bootstrap_cluster.sh"`

2) SSH EMR Cluster. 

`ssh hadoop@ec2-*.compute.amazonaws.com`  <br />

Run the following commands <br />
`airflow initdb`              Initiate the Airflow DAG <br />
`airflow webserver -p 8082`   Start the Airflow Webserver <br />
`airflow scheduler`           Start the Airflow scheduler <br />

3) Airflow Webserver <br />

Note: Need to change PROJECTID <br />
Go to Airflow webserver in your browser. <br />

`ec2-*.compute.amazonaws.com:8082` <br />
  i) Go to Admin -> Connections and edit bigquery_default <br />
  `Conn_type` Google Cloud Platform   <br />
  `Project ID` PROJECTID             <br />
  `Keyfile Path` /home/hadoop/setup_files/client_secret.json  <br />
  `Scopes (comma seperated)` https://www.googleapis.com/auth/cloud-platform <br />
  And finally click on Save <br />
  
  ii) Click on DAGs tab in Airflow Webserver <br />
  Enable `pipeline_example` <br />
  And now the pipeline should run all the tasks listed in pipeline_example.py <br />
