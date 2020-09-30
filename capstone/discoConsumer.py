from kafka import KafkaConsumer

consumer = KafkaConsumer('track_id',bootstrap_servers='localhost:9099')

for msg in consumer:
	x = msg.value.decode()
	f = open('track.txt','w')
	print(x, file=f)
	exit()
