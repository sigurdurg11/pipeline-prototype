# Bootstrap Cluster
# Author: Sigurdur Geirsson
# Last modified 2018/07/04

echo '
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
export GOOGLE_APPLICATION_CREDENTIALS=/home/hadoop/setup_files/client_secret.json
' >> ~/.bashrc

# Amazon Authentication
aws s3 cp s3://BUCKET/PATH/s3_credentials.tar /home/hadoop/s3_credentials.tar # <- PATH change
tar xvf /home/hadoop/s3_credentials.tar
cd .aws
sed 's/ //g' credentials > credentials2
key_id=$(echo "$a" | sed '2q;d' credentials2)
access_key=$(echo "$a" | sed '3q;d' credentials2)
rm credentials2
echo "export $key_id" >> ~/.bashrc
echo "export $access_key" >> ~/.bashrc
cd /home/hadoop/

# Google Authentication
aws s3 cp s3://BUCKET/PATH/client_secret.json /home/hadoop/setup_files/client_secret.json  # <- PATH change

# Install various requirements
sudo yum install -y tmux git ack htop  
sudo pip install oauth2client
sudo pip install google-api-python-client
sudo yum install -y python36 python35-devel python36-pip python36-setuptools python36-virtualenv
sudo pip-3.6 install --upgrade pip

# Setup Spark
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
source ~/.bashrc

# Setup Airflow
sudo yum install -y gcc-c++ python-devel python-setuptools openldap-devel;
sudo yum -y update
sudo yum -y install mysql-devel.noarch 
sudo pip install cryptography 
sudo pip install apache-airflow[async,devel,celery,crypto,druid,gcp_api,jdbc,hdfs,hive,kerberos,ldap,password,postgres,qds,rabbitmq,s3,samba,slack]==1.9.0

# Setup Airflow Plugins
wget https://raw.githubusercontent.com/apache/incubator-airflow/master/airflow/operators/bash_operator.py -P /home/hadoop/airflow/plugins/
wget https://raw.githubusercontent.com/rssanders3/airflow-spark-operator-plugin/master/spark_operator_plugin.py -P /home/hadoop/airflow/plugins/

mkdir /home/hadoop/airflow/spark_jobs/

# Copy Spark jobs and Airflow DAGs from S3
aws s3 cp s3://BUCKET/PATH/preprocessing_s3.py /home/hadoop/airflow/spark_jobs/preprocessing.py # <- PATH change
aws s3 cp s3://BUCKET/PATH/s3_to_bq.py /home/hadoop/airflow/spark_jobs/s3_to_bq.py # <- PATH change
aws s3 cp s3://BUCKET/PATH/bq_to_s3.py /home/hadoop/airflow/spark_jobs/bq_to_s3.py # <- PATH change
aws s3 cp s3://BUCKET/PATH/pipeline_example.py /home/hadoop/airflow/dags/pipeline_example.py # <- PATH change

sudo pip install pandas-gbq
sudo pip install --upgrade google-cloud-bigquery
sudo pip install markupsafe==0.23
sudo pip install py4j

echo 'export SPARK_HOME=/usr/lib/spark/' >> ~/.bashrc
echo 'PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
echo 'PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH' >> ~/.bashrc
echo 'PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.6-src:$PYTHONPATH' >> ~/.bashrc
echo 'export PYTHONPATH' >> ~/.bashrc
source ~/.bashrc
echo 'export CLOUDSDK_PYTHON=python2.7' >> ~/.bashrc

# Update Guava (For Google Access)
sudo rm /usr/lib/spark/jars/guava-*
sudo aws s3 cp s3://BUCKET/PATH/guava-24.1-jre.jar /usr/lib/spark/jars/guava-24.1-jre.jar # <- PATH change

# Setup Google Cloud SDK
aws s3 cp s3://BUCKET/PATH/google-cloud-sdk-204.0.0-linux-x86_64.tar.gz /home/hadoop/google-cloud-sdk-204.0.0-linux-x86_64.tar.gz
tar xfv /home/hadoop/google-cloud-sdk-204.0.0-linux-x86_64.tar.gz
/home/hadoop/google-cloud-sdk/install.sh -q
echo 'export PATH=/home/hadoop/google-cloud-sdk/bin:$PATH' >> ~/.bashrc
sudo rm /home/hadoop/google-cloud-sdk-204.0.0-linux-x86_64.tar.gz
source ~/.bashrc
/home/hadoop/google-cloud-sdk/bin/gcloud auth activate-service-account PROJECTID-cloud-storage@PROJECTID.iam.gserviceaccount.com --key-file /home/hadoop/setup_files/client_secret.json	# <- Change needed(PROJECTID)

gcloud config set project PROJECTID # <- Change needed(PROJECTID)
source ~/.bashrc
sudo python3 -m pip install google-cloud-bigquery


