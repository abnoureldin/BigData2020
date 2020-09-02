from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="shakespeare")
ssc = StreamingContext(sc,2)

txt = sc.textFile("./Shakespeare.txt")
dks = KafkaUtils.createDirectStream(ssc,["shakespeare"],
					{"metadata.broker.list":"localhost:9099"})

counts = dks.pprint()

dks.count().pprint()

ssc.start()
ssc.awaitTermination()
