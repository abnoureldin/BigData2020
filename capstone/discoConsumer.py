from kafka import KafkaConsumer

consumer = KafkaConsumer('track_id',bootstrap_servers='localhost:9099')

for msg in consumer:
	print(msg.value.decode())
