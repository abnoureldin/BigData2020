from musixmatch import Musixmatch

api = str(open("musixmatch.txt","r").read().strip())
musixmatch = Musixmatch(api)

with open("track.txt", "r") as lst:
    tracks  = lst.readlines()

for t in tracks:
	l = t.split(",")

output = []

for i in l:
	output.append(i.strip("[")\
			.strip("]")\
			.strip("\n"))

for i in output:
	print(i)








