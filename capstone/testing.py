import json

x = json.load(open("data.json"))
print(x['result']['spotify']['id'])
_list = x['result']['spotify']['album']['artists']
_dict = {k:v for d in _list for k,v in d.items()}
print(_dict['id'])