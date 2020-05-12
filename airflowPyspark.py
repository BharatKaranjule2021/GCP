# airflowPyspark.py
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
import os

sparkJob1 = 'gcloud dataproc jobs submit pyspark gs://python-script-assignment/readbigquery.py --cluster dataproc-hive-cluster --region us-east1 --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar'

sparkJob2 = 'gcloud dataproc jobs submit pyspark gs://python-script-assignment/readfile_spark.py --cluster dataproc-hive-cluster --region us-east1 --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar'

mapreducejob = "gcloud dataproc jobs submit hadoop --cluster dataproc-hive-cluster --region us-east1 --jar gs://python-script-assignment/WordCount.jar -- com.WordCount gs://mapreduce-input-output/input/ gs://mapreduce-input-output/output/"

default_args = {
	'owner': 'Bharat',
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Run-my-Airflow-Scripts',
    default_args=default_args,
    description='Running multiple scripts',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60))

readfile= BashOperator(
    task_id='readfile-data',
    bash_command=sparkJob1,
    dag=dag)

readbigquery = BashOperator(
    task_id='readbigquery-data',
    bash_command=sparkJob2,
    dag=dag)

mapreduce = BashOperator(
    task_id='mapreduce-Job',
    bash_command=mapreducejob,
    dag=dag)
	
readbigquery.set_upstream(readfile)


readfile >> mapreduce >> readbigquery
