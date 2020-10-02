from musixmatch import Musixmatch
import json

x = json.load(open("data.json"))

artist = x['result']['artist']

api = str(open("musixmatch.txt","r").read().strip())
musixmatch = Musixmatch(api)

with open("track.txt", "r") as lst:
    tracks  = lst.readlines()

for t in tracks:
	l = t.split(",")

output = []

for i in l:
	output.append(i.strip("\[\]\n\ "))

#for i in output:
#	print(i)

disco = []

for i,x in enumerate(output):
	try:
		body = musixmatch.matcher_lyrics_get(x,artist)
		lyrics = body['message']['body']['lyrics']['lyrics_body'].replace("******* This Lyrics is NOT for Commercial use *******",'')
		disco.append(lyrics)
	except:
		None

print(disco)







