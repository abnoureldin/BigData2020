import json
import requests
import sys
import threading
import pandas as pd
from kafka import KafkaProducer

pair = 'XXBTZUSD'

def kraken(pair):
	url = "https://api.kraken.com/0/public/OHLC"
	link = url + "?pair=" + pair
	r = requests.get(link)
	trade = r.json()['result'][pair]
	dicts = []
	for num,value in enumerate(trade):
		time = trade[num][0]
		open = trade[num][1]
		high = trade[num][2]
		low = trade[num][3]
		close = trade[num][4]
		vwap = trade[num][5]
		volume = trade[num][6]
		count = trade[num][7]
		d = {'time':time,'open':open,
				'high':high,'low':low,
				'close':close,'vwap':vwap,
				'volume':volume,'count':count}
		
		dicts.append(d)
	dicts  = json.dumps(dicts)
	return dicts

producer = KafkaProducer(bootstrap_servers='localhost:9099')

def stream():
	threading.Timer(60.0,stream).start()
	producer.send('kraken',kraken(pair).encode('utf-8'))
	producer.flush()


if __name__ == "__main__":
	stream()

