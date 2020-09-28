from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

if __name__ == "__main__":
	sc = SparkContext(appName='audio-brain')
	ssc = StreamingContext(sc,60)
	sc.setLogLevel("WARN")
	broker,topic = "localhost:9099","audio-brain"
	kvs = KafkaUtils.createDirectStream(ssc,[topic],
							{"metadata.broker.list":broker})
	lines = kvs.map(lambda x: x[1])
	lines.pprint()
	ssc.start()
	ssc.awaitTermination()