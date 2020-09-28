import requests
import json
from kafka import KafkaProducer
import boto3
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("-f", type=str, help="File Location.")
args = parser.parse_args()

def upload(filename):
	if not args.f:
		print("No file location specified. Provide -f <filepath>.")
		exit()
	name = filename.split('/')[-1]
	name = name.strip().lower()
	aws_access = str(open("aws_access.txt","r").read().strip())
	aws_secret = str(open("aws_secret.txt","r").read().strip())
	session = boto3.Session(
    	aws_access_key_id=aws_access,
    	aws_secret_access_key=aws_secret,
    	region_name='eu-west-2'
		)
	s3 = session.resource('s3')
	s3.meta.client.upload_file(filename, 'audio-brain', name,
				 ExtraArgs={'ACL':'public-read'})
	print("File uploaded successfully.")
	global file
	file = "https://audio-brain.s3.eu-west-2.amazonaws.com/"+name

def audio(url):
	print("Detecting match...")
	token = str(open("api.txt","r").read().strip())

	data = {
    	'url': url,
    	'return': 'spotify',
    	'api_token': token
	}
	result = requests.post('https://api.audd.io/', data=data)
	print("Match status:")
	parsed = json.loads(result.text)
	with open('data.json', 'w', encoding='utf-8') as f:
		json.dump(parsed, f, ensure_ascii=False, indent=4)


def producer(filename):
	data = json.load(open(filename))
	if data['status'] == "success":
		data = json.dumps(data)
		
		producer = KafkaProducer(bootstrap_servers="localhost:9099")
		producer.send("audio-brain",data.encode("utf-8"))
		producer.flush()
		print("Stream sent.")
	else:
		print("Stream failed.")

if __name__ == "__main__":
	#upload(args.f)
	#audio(file)
	producer('data.json')