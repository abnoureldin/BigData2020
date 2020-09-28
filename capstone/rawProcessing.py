# -*- coding: utf-8 -*-

from pydub import AudioSegment
from pydub.utils import make_chunks
import pydoop.hdfs as hdfs
from subprocess import PIPE, Popen
import requests
import base64
import http.client

def audioConverter(filename):
	name = filename.split('.')[0]
	ext = filename.split('.')[1] 
	sound = AudioSegment.from_file(name+'.'+ext, ext)
	sound = sound.set_channels(1)
	chunk_length_ms = 1000
	chunks = make_chunks(sound, chunk_length_ms)
	for i, chunk in enumerate(chunks):
		raw_data = chunk.raw_data
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
	

#audioConverter("/home/ab/BigData2020/capstone/samples/test.mp3")
