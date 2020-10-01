from kafka import KafkaConsumer

consumer = KafkaConsumer('audio-output',bootstrap_servers='localhost:9099')

for msg in consumer:
    x = msg.value.decode()
    f = open('track.txt','w')
    print(x, file=f)
    print("track list saved as track.txt")
    exit()