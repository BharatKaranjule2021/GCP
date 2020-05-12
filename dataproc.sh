#!/bin/bash

project_name="dataproc-assignment"
log=dataproc-assignment_`date '+%Y-%m-%d'`.log

gsutil ls -L -b gs://dataproc-cluster-poc-bucket/ >>$log
return_value=$?
echo "$return_value"
if [ $return_value = 0 ]; then
    echo "Bucket exist"
else
    echo "bucket does not exist creating bucket"
	gsutil mb gs://dataproc-cluster-poc-bucket/ >>$log
	if [$? = 0] ; then
		echo "New Bucket Created successfully"
	else
		echo "New Bucket Not Created .Process end "
		exit 1
	fi
fi


#Create Cluster
echo "Creating Dataproc Cluster"
gcloud dataproc clusters create mysql-to-dataproc-hive-table --bucket dataproc-cluster-poc-bucket --region us-central1 --subnet default --zone us-central1-a --master-machine-type n1-standard-1 --master-boot-disk-size 50 --num-workers 2 --worker-machine-type n1-standard-1 --worker-boot-disk-size 50 --project dataproc-assignment >>$log
if [ $? -eq 0 ]; then
    echo "Creating Dataproc Cluster successfully" >>$log
else
    echo "Creating Dataproc Cluster .Process end " >>$log
	exit 1
fi
sleep 10

#execute Mapreduce Job in dataproc
gcloud dataproc jobs submit hadoop --cluster=mysql-to-dataproc-hive-table --region us-central1 --jar gs://dataproc-cluster-wordcount/WordCount.jar -- com.WordCount gs://mapreduce-input-output/input/ gs://mapreduce-input-output/output/ >>$log
if [ $? -eq 0 ]; then
    echo "Mapreduce Job execution successfully" >>$log
else
    echo "Mapreduce Job execution fail" >>$log
	exit 1
fi

sleep 10

#Delete in dataproc
gcloud dataproc clusters delete mysql-to-dataproc-hive-table --region=us-central1 >>$log
if [ $? -eq 0 ]; then
    echo "Deleted cluster successfully" >>$log
else
    echo "Deleted cluster operation is failed" >>$log
	exit 1
fi

echo "End Of Script"



