import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
from kafka import KafkaProducer

cid = str(open("cid.txt","r").read())
secret = str(open("secret.txt","r").read())

client_credentials_manager = SpotifyClientCredentials(client_id=cid,
				client_secret=secret)
spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

uri = 'spotify:artist:0k17h0D3J5VfsdmQ1iZtE9'

results = spotify.artist_top_tracks(uri)

track_list = []
audio_list = []
art_list = []

for track in results['tracks'][:50]:
	track_list.append(track['name'])
	audio_list.append(track['preview_url'])
	art_list.append(track['album']['images'][0]['url'])

d =  {'track':track_list,
	'audio':audio_list,
	'artwork':art_list}

x = json.dumps(d)

servers = 'sandbox-hdp.hortonworks.com:6667'
topic = 'spotify'
producer = KafkaProducer(bootstrap_servers = servers)
producer.send(topic,x.encode('utf-8'))
producer.flush()


