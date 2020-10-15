import pymongo
from pymongo import MongoClient
import re
import os
from os import path
from wordcloud import WordCloud
import matplotlib.pyplot as plt

client = MongoClient('mongodb://localhost:27017/')

db = client.audio_brain

lyrics = db.lyrics.find()

disco = []

for words in lyrics:
	x = '{0} {1}'.format(words['_id'], words['value'])
	disco.append(x)


cleaned = []

for i in disco:
	lines = i.replace("\n",' ')
	lines = lines.replace("(",'')
	lines = lines.replace(")",'')
	lines = lines.replace("...",'')
	lines = lines.replace(":",'')
	lines = lines.lower()
	lines = lines.replace("fucking",'')
	clean = re.sub(r'\w*\d\w*', '', lines).strip()
	cleaned.append(clean)


# Generate a word cloud image
wordcloud = WordCloud().generate(str(cleaned))

plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()
