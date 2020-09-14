from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, from_json
from pyspark.sql.types import *


if __name__=="__main__":
	bootstrapServers = "localhost:9099"
	topics = "kraken"
	subscribeType = "subscribe"
	
	spark = SparkSession\
		.builder\
		.appName("StructuredBitcoin")\
		.config("spark.sql.warehouse.dir","/user/hive/warehouse")\
		.config("hive.metastore.uris","thrift://localhost:9083")\
		.enableHiveSupport()\
		.getOrCreate()

	data = spark\
		.readStream\
		.format("kafka")\
		.option("kafka.bootstrap.servers",bootstrapServers)\
		.option(subscribeType, topics)\
		.load()
	
	#schema = spark.read.option("multiLine",True)\
	#	.format("json")\
	#	.load("file:////home/ab/BigData2020/project1/ohlc.txt")\
	#	.schema
	
	schema = StructType([
		StructField("time",TimestampType()),
		StructField("open",DoubleType()),
		StructField("high",DoubleType()),
		StructField("low",DoubleType()),
		StructField("close",DoubleType()),
		StructField("vwap",DoubleType()),
		StructField("volume",DoubleType()),
		StructField("count",DoubleType())
		])

	df = data.select(from_json(col("value").cast("string"), schema)\
		.alias("bitcoin")).selectExpr("bitcoin.*")
	
	def write(df,epoch_id):
		df.show()
		df.write.mode("append").saveAsTable("bitcoin.data")

	query = df.writeStream.foreachBatch(write).start()

	query.awaitTermination()
	
	
