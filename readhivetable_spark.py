from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext

sparkSession = (SparkSession.builder.appName('example-pyspark-read-data-from-hive').enableHiveSupport().getOrCreate())

df_load = sparkSession.sql('SELECT * FROM hive_dataproc.payment limit 5')
df_load.show()