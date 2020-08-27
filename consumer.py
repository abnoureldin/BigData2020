from pykafka import KafkaClient

localhost = 'localhost:9099'
client = KafkaClient(hosts=localhost)
topic = client.topics['bigdata']
consumer = topic.get_simple_consumer()
for message in consumer:
	if message is not None:
		print(message.offset, message.value.decode())
