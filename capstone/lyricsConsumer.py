import pymongo
from pymongo import MongoClient
import re

client = MongoClient('mongodb://localhost:27017/')

db = client.audio_brain

lyrics = db.lyrics.find()

disco = []

for words in lyrics:
	x = '{0} {1}'.format(words['_id'], words['value'])
	disco.append(x)

cleaned = []

for i in disco:
	lines = i.replace("\n",'')
	clean = re.sub(r'\w*\d\w*', '', lines).strip()
	cleaned.append(clean)

print(cleaned)
