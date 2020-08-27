from pykafka import KafkaClient
import string

text = open('/home/hadoop/BigData2020/Shakespeare.txt','r').read()
text.translate(str.maketrans('','',string.punctuation))
text = text.split()
localhost = 'localhost:9099'
client = KafkaClient(hosts=localhost)
topic = client.topics['bigdata']
producer = topic.get_sync_producer()
for i in text:
	producer.produce(i.encode('ascii'))
