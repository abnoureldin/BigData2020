from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

def main():
	sc = SparkContext(appName='Bitcoin')
	ssc = StreamingContext(sc,60)
	global ss
	ss = SparkSession.builder.appName("Bitcoin")\
		.config("spark.sql.warehouse.dir","/user/hive/warehouse")\
		.config("hive.metastore.uris","thrift://sandbox-hdp.hortonworks.com:9083")\
		.enableHiveSupport().getOrCreate()
	global sqlContext
	sqlContext= SQLContext(sc)
	sc.setLogLevel("WARN")
	broker,topic = "sandbox-hdp.hortonworks.com:6667","kraken"
	kvs = KafkaUtils.createDirectStream(ssc,[topic],
					{"metadata.broker.list":broker})
	lines = kvs.map(lambda x: x[1])
	lines.foreachRDD(lambda rdd: readRDD(rdd))
	ssc.start()
	ssc.awaitTermination()

def readRDD(RDD):
	if not RDD.isEmpty():
		df = sqlContext.read.json(RDD)
		df.registerTempTable("Bitcoin_OHLC")
		cols = ['time','open',
                                'high','low',
                                'close','vwap',
                                'volume','count']
		data = sqlContext.sql(
				"SELECT "+
				",".join(cols)+
				" FROM Bitcoin_OHLC WHERE close IS NOT NULL")
		data.show()
		data.write.saveAsTable(name="bitcoin.datastream",
				format="hive",mode="append")
if __name__ == "__main__":
	main()
