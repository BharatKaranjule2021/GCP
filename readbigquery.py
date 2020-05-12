#!/usr/bin/python
"""BigQuery I/O PySpark example."""
from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.

print("Spark session created")

bucket = "pyspark-bigquery-demo"
spark.conf.set('temporaryGcsBucket', bucket)

print("Spark conf done")

# Load data from BigQuery.
mytable = spark.read.format('bigquery').option('table', 'dataproc-assignment:mydataset.temptable').load()
mytable.createOrReplaceTempView('temptable')
mytable.show(5)

# Perform Operation.
temptable2 = spark.sql('SELECT * FROM temptable limit 4')
temptable2.show()
temptable2.printSchema()

# Saving the data to BigQuery
temptable2.write.format('bigquery') \
  .option('table', 'dataproc-assignment:mydataset.temptable2') \
  .save()