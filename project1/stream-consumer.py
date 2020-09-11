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
	
	
	schema = StructType([
		StructField("time",TimestampType()),
		StructField("open",IntegerType()),
		StructField("high",IntegerType()),
		StructField("low",IntegerType()),
		StructField("close",IntegerType()),
		StructField("vwap",IntegerType()),
		StructField("volume",IntegerType()),
		StructField("count",IntegerType())
		])

	df = data.select(from_json(col("value").cast("string"), schema)\
		.alias("bitcoin")).selectExpr("bitcoin .*")
	
	def write(df,epoch_id):
		df.show()
		df.write.mode("append").saveAsTable("bitcoin.data")

	query = df.writeStream.foreachBatch(write).start()

	query.awaitTermination()
	
	
