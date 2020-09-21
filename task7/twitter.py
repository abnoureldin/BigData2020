#spark-submit --jars /opt/mysql-connector-java-8.0.21.jar twitter.py 
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.sql.functions import *
import re

spark = SparkSession.builder.appName("twitter").getOrCreate()
ssc = StreamingContext(spark.sparkContext,5)
lines = ssc.textFileStream("hdfs://localhost:9000/twitter")

def transform(rdd):
	if not rdd.isEmpty():
		df = spark.createDataFrame(rdd,schema=["#","count"])
		SQL(df)

def SQL(df):
	df.show()
	df.write.format("jdbc").mode("overwrite")\
	.option("driver","com.mysql.jdbc.Driver")\
	.option("url","jdbc:mysql://localhost:3306/bigdata2020")\
	.option("dbtable","tweets")\
	.option("user","root")\
	.option("password","password")\
	.save()

counts = lines.flatMap(lambda l:l.split(' ')) \
    .filter(lambda w:w.lower().startswith('#')) \
    .map(lambda w: w.replace('#','')) \
    .map(lambda w: w.lower()) \
    .filter(lambda w: re.sub(r'[^a-z]+','',w)) \
    .filter(lambda w: re.sub(r'[^\x00-\x7F]+','',w)) \
    .map(lambda x: (x,1)) \
    .reduceByKey(lambda x,y:x+y) \
    .map(lambda r: (r[0],r[1])) \
    .foreachRDD(transform)

ssc.start()
ssc.awaitTermination()
