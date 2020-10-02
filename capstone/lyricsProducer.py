from musixmatch import Musixmatch
import json
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from kafka import KafkaProducer

"""
spark-submit --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/audio_brain.lyrics?readPreference=primaryPreferred" \
             --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/audio_brain.lyrics" \
             --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.4 \
             --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 lyricsProducer.py
"""

x = json.load(open("data.json"))

artist = x['result']['artist']

api = str(open("musixmatch.txt","r").read().strip())
musixmatch = Musixmatch(api)

with open("track.txt", "r") as lst:
    tracks  = lst.readlines()

for t in tracks:
	l = t.split(",")

output = []

for i in l:
	output.append(i.strip("\[\]\n\ "))

#for i in output:
#	print(i)

disco = []

for i,x in enumerate(output):
	try:
		body = musixmatch.matcher_lyrics_get(x,artist)
		lyrics = body['message']['body']['lyrics']['lyrics_body'].replace("******* This Lyrics is NOT for Commercial use *******",'')
		disco.append(lyrics)
	except:
		None

f = open("lyrics.txt","w")
print(disco,file=f)

data = json.dumps(disco)

producer = KafkaProducer(bootstrap_servers="localhost:9099")
producer.send("audio-lyrics",data.encode("utf-8"))
producer.flush()


spark = SparkSession.builder.appName("audio_brain")\
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/audio_brain.lyrics")\
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/audio_brain.lyrics")\
    .getOrCreate()

result = spark.createDataFrame(disco, StringType())

result.write.format("mongo").mode("overwrite")\
    .option("database","audio_brain")\
    .option("collection", "lyrics").save()

result.show()







