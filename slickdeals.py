#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import pandas as pd

df = pd.DataFrame()
r = requests.get("https://slickdeals.net/laptop-deals/")
text = r.content

soup = BeautifulSoup(text, "html.parser")

items = []
prices = []

for i in soup.find_all('div', class_='itemImageAndName'):
    item = i.find('a', class_='itemTitle bp-c-link')
    items.append(item.text)
    price = i.find('img')
    prices.append(str(price))

df['item'] = items
df['price'] = prices   

clean=[]

for i,j in enumerate(df['price']):
    clean.append(df['price'][i][5:15])
    
df['price'] = clean
df['price'] = df['price'].str.split('=').str.get(0)

df.to_csv('output.csv')

    
    


