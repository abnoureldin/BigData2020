from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession

ss = SparkSession.builder.appName("tweets").enableHiveSupport().getOrCreate()

df = ss.read.format("avro").load("hdfs://localhost:9000/user/twitter/FlumeData.1599577737180")
#df = ss.read.format("avro").load("file:///home/ab/Downloads/twitter.avro")
df.show()
