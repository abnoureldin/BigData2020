
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode

sc = SparkContext(appName='Spotify')
ssc = StreamingContext(sc,2)
ss = SparkSession.builder.appName("Spotify").getOrCreate()
sqlContext = SQLContext(sc)
sc.setLogLevel("WARN")
broker,topic = "sandbox-hdp.hortonworks.com:6667","spotify"
kvs = KafkaUtils.createDirectStream(ssc,[topic],
				{"metadata.broker.list":broker})
lines = kvs.map(lambda x: x[1])

def _RDD(RDD):
        if not RDD.isEmpty():
		sink = "org.apache.hadoop.hbase.spark"
		catalog = ''.join("""{
    		"table":{"namespace":"default", "name":"spotify"},
    		"rowkey":"key",
    		"columns":{
        		"track":{"cf":"rowkey", "col":"track", "type":"string"},
        		"audio":{"cf":"cf", "col":"audio", "type":"string"},
			"artwork":{"cf":"cf","col":"artwork","type":string}
   		 	}
			}""".split())
		df = sqlContext.read\
			.option("multiline",True).json(RDD)
                df.registerTempTable("spotify_data")
                cols = ['track','audio','artwork']

                data = sqlContext.sql("SELECT "+
                                        ",".join(cols)+
                                        " FROM spotify_data")
                DF = data.withColumn("track",explode(data.track))
		DF.write.options(catalog=catalog)\
			.format(sink).save()

lines.foreachRDD(lambda x: _RDD(x))

ssc.start()
ssc.awaitTermination()			
