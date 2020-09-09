from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

def main():
	sc = SparkContext(appName='Bitcoin')
	ssc = StreamingContext(sc,60)
	global sqlContext
	sqlContext= SQLContext(sc)
	sc.setLogLevel("WARN")
	broker,topic = "localhost:9099","kraken"
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

if __name__ == "__main__":
	main()
