from pyspark import SparkContext
from pyspark import SparkConf

# create Spark context with Spark configuration
conf = SparkConf().setAppName("read text file in pyspark")
sc = SparkContext(conf=conf)

# Read file into RDD
lines = sc.textFile("gs://bigquery-test-data-1/sampledatav1.csv")

# Call collect() to get all data
llist = lines.collect()

# print line one by line
for line in llist:
	print(line)