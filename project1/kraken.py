import json
import requests
from datetime import datetime
import threading

ticker = 'XXBTZUSD'

def kraken(ticker):
	url = "https://api.kraken.com/0/public/Ticker"
	link = url + "?pair=" + ticker
	r = requests.get(link)
	last_trade = r.json()['result'][ticker]['c']
	for num in last_trade:
		btc = last_trade[0]
		size = last_trade[1]
		x = ("last trade: %s BTC at %s USD as at %s")%(
						size, btc, datetime.now())
		return x


def stream():
	threading.Timer(5.0,stream).start()
	print(kraken(ticker))

stream()
