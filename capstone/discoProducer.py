import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json
from kafka import KafkaProducer

cid = str(open("cid.txt","r").read())
secret = str(open("secret.txt","r").read())

client_credentials_manager = SpotifyClientCredentials(client_id=cid,
        client_secret=secret)
spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

x = json.load(open("data.json"))
track_id = x['result']['spotify']['id']
track_uri = x['result']['spotify']['uri']
_list = x['result']['spotify']['album']['artists']
_dict = {k:v for d in _list for k,v in d.items()}
artist_id = _dict['id']
artist_uri = _dict['uri']

uri = 'spotify:track:'
print(artist_id,artist_uri,track_id,track_uri)




