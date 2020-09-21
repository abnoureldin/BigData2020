
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

sc = SparkContext(appName='Spotify')
ssc = StreamingContext(sc,2)
ss = SparkSession.builder.appName("Spotify").getOrCreate()
sqlContext = SQLContext(sc)
sc.setLogLevel("WARN")
broker,topic = "sandbox-hdp.hortonworks.com:6667","spotify"
kvs = KafkaUtils.createDirectStream(ssc,[topic],
				{"metadata.broker.list":broker})
lines = kvs.map(lambda x: x[1])

def readRDD(RDD):
        if not RDD.isEmpty():
                df = sqlContext.read\
			.option("multiline",true).json(RDD)
                df.registerTempTable("spotify_data")
                cols = ['track','audio','artwork']

                data = sqlContext.sql("SELECT "+
                                        ",".join(cols)+
                                        " FROM spotify_data")
                data.show()

lines.foreachRDD(lambda rdd: readRDD(rdd))
ssc.start()
ssc.awaitTermination()			
