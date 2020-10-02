import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from kafka import KafkaProducer

"""
spark-submit --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/audio_brain.output?readPreference=primaryPreferred" \
             --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/audio_brain.output" \
             --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.4 \
             --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 discoProducer.py
"""

cid = str(open("cid.txt","r").read().strip())
secret = str(open("secret.txt","r").read().strip())

client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
x = json.load(open("data.json"))

track_id = x['result']['spotify']['id']
track_uri = x['result']['spotify']['uri']
_list = x['result']['spotify']['album']['artists']
_dict = {k:v for d in _list for k,v in d.items()}
artist_id = _dict['id']
artist_uri = _dict['uri']

#print("artist_uri: ", artist_uri+"\n","track_uri: ", track_uri)

top_tracks = spotify.artist_top_tracks(artist_uri)

track_list = []

for track in top_tracks['tracks']:
  track_list.append(track['name'])

artist = spotify.artist(artist_uri)

output = []

for i in track_list:
  output.append(i)

data = json.dumps(output)

producer = KafkaProducer(bootstrap_servers="localhost:9099")
producer.send("audio-output",data.encode("utf-8"))
producer.flush()


spark = SparkSession.builder.appName("audio_brain")\
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/audio_brain.output")\
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/audio_brain.output")\
    .getOrCreate()

result = spark.createDataFrame(output, StringType())

result.write.format("mongo").mode("overwrite")\
    .option("database","audio_brain")\
    .option("collection", "output").save()

result.show()




"""db.output.find({},{value:1,_id:0})"""