import pymongo
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

db = client.audio_brain

lyrics = db.lyrics.find()

disco = []

for words in lyrics:
	x = '{0} {1}'.format(words['_id'], words['value'])
	disco.append(x)

print(disco)
