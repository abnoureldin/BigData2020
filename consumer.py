from kafka import KafkaConsumer

consumer = KafkaConsumer('shakespeare',bootstrap_servers='localhost:9099')

for msg in consumer:
	print(msg.value.decode())
