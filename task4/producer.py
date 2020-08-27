from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9099')

with open('/home/hadoop/BigData2020/Shakespeare.txt') as f:
	for line in f:
		producer.send('shakespeare',line.encode('ascii'))

producer.flush()
producer.close()
