# -*- coding: utf-8 -*-

from pydub import AudioSegment
import pydoop.hdfs as hdfs
from subprocess import PIPE, Popen

def audioConverter(filename):
	name = filename.split('.')[0]
	ext = filename.split('.')[1]
	if ext == 'mp3': 
		sound = AudioSegment.from_mp3(name+'.'+ext)
		raw_data = sound._data

		with open(name+'.'+'raw','wb') as file:
			file.write(raw_data)
		print("File converted successfully from mp3 to raw.")
		try:
			put = Popen(["hdfs", "dfs", "-put", name+'.'+'raw', '/audio'], stdin=PIPE, bufsize=-1)
			put.communicate()
			print("File added to HDFS.")
		except:
			print("Something went wrong.")
		global n
		n = name.split('/')[-1]+'.raw'
	
	elif ext == 'wav': 
		sound = AudioSegment.from_wav(name+'.'+ext)
		raw_data = sound._data

		with open(name+'.'+'raw','wb') as file:
			file.write(raw_data)
		print("File converted successfully from wav to raw.")
		try:
			put = Popen(["hdfs", "dfs", "-put", name+'.'+'raw', '/audio'], stdin=PIPE, bufsize=-1)
			put.communicate()
			print("File added to HDFS.")
		except:
			print("Something went wrong.")
		n = name.split('/')[-1]+'.raw'


audioConverter("/home/ab/BigData2020/capstone/samples/Pink_Floyd_Demo.mp3")

with hdfs.open('/audio/'+n) as f:
    for line in f:
        print(line)