from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, to_json, struct



"""
spark-submit --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/audio_brain.result?readPreference=primaryPreferred" \
             --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/audio_brain.result" \
             --packages org.mongodb.spark:mongo-spark-connector_2.11:2.3.4 \
			 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 discoProducer.py

"""

spark = SparkSession.builder.appName("audio_brain")\
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/audio_brain.result")\
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/audio_brain.result")\
    .getOrCreate()

df = spark.read.format("mongo").load()
df = df.select("spotify")
id = df.select('spotify.*')

ds = id \
  .select(to_json(struct("id")).alias("value")) \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9099") \
  .option("topic", "track_id") \
  .save()


"""
SCHEMA
album, artists, disc_number, duration_ms, explicit,
external_ids, external_urls, href, id, is_local,
name, popularity, preview_url, track_number, type, uri
"""
