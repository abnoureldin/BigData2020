import json
import requests
import sys
import threading
import pandas as pd
from kafka import KafkaProducer

pair = 'XXBTZUSD'

def kraken(ticker):
	url = "https://api.kraken.com/0/public/OHLC"
	link = url + "?pair=" + pair
	r = requests.get(link)
	trade = r.json()['result'][pair]
	for num in trade:
		time = trade[0]
		open = trade[1]
		high = trade[2]
		low = trade[3]
		close = trade[4]
		vwap = trade[5]
		volume = trade[6]
		count = trade[7]
		x = '''time:%s,
			open:%s,
			high:%s,
			low:%s,
			close:%s,
			vwap:%s,
			volume:%s,
			count:%s'''%(time,open,high,low,close,vwap,volume,count)
		return x

producer = KafkaProducer(bootstrap_servers='localhost:9099')

def stream():
	threading.Timer(5.0,stream).start()
	producer.send('kraken',kraken(pair).encode('utf-8'))
	producer.flush()

stream()
