from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import from_json, col, explode
"""
spark-submit --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/audio-brain.result?readPreference=primaryPreferred" \
             --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/audio-brain.result" \
             --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 \
			 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 audioConsumer.py
"""

def readRDD(rdd):
	if not rdd.isEmpty():
		df = sqlContext.read.option("multiLine",True).json(rdd)
		
		df.registerTempTable("audio_brain")
		cols = ["result"]
		data = sqlContext.sql("SELECT "+
							",".join(cols)+
							" FROM audio_brain")

		result = data.select("result.*")
		result.write.format("mongo")\
		.mode("append").save()

if __name__ == "__main__":
	working_directory = "jars/*"
	sc = SparkContext(appName='audio-brain')
	ssc = StreamingContext(sc,60)
	sqlContext = SQLContext(sc)
	spark = SparkSession(sc)
	sc.setLogLevel("WARN")
	ss = SparkSession.builder.appName("audio-brain")\
		.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/audio-brain.result")\
		.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/audio-brain.result")\
		.getOrCreate()
	
	broker,topic = "localhost:9099","audio-brain"
	
	kvs = KafkaUtils.createDirectStream(ssc,[topic],
							{"metadata.broker.list":broker})
	
	lines = kvs.map(lambda x: x[1])
	rdd = lines.foreachRDD(readRDD)
	#json_schema = spark.read.json(lines.rdd.map(lambda row: row.json)).schema
	#df.withColumn('json', from_json(col('json'), json_schema))

	ssc.start()
	ssc.awaitTermination()