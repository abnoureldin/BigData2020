from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def main():
	sc = SparkContext(appName='Shakespeare')
	ssc = StreamingContext(sc,2)
	brokers,topic = "localhost:9099","shakespeare"
	kvs = KafkaUtils.createDirectStream(ssc,[topic],{
					"metadata.broker.list":brokers})
	lines = kvs.map(lambda x: x[1])
	transform = lines.map(lambda l: (l,int(len(l.split())),int(len(l))))
	transform.pprint()
	ssc.start()
	ssc.awaitTermination()

if __name__ == "__main__":
	main()
