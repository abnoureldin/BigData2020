from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def main():
	sc = SparkContext(appName='Bitcoin')
	ssc = StreamingContext(sc,2)
	broker,topic = "localhost:9099","kraken"
	kvs = KafkaUtils.createDirectStream(ssc,[topic],
					{"metadata.broker.list":broker})
	lines = kvs.map(lambda x: x[1])
	lines.foreachRDD(lambda rdd: rdd.toDF())
	lines.pprint()
	ssc.start()
	ssc.awaitTermination()

if __name__ == "__main__":
	main()
