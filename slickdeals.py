#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

df = pd.DataFrame()
r = requests.get("https://slickdeals.net/laptop-deals/")
text = r.content

soup = BeautifulSoup(text, "html.parser")

items = []
prices = []

for i in soup.find_all('div', class_='itemImageAndName'):
    item = i.find('a', class_='itemTitle')
    items.append(item.text)

for j in soup.find_all('div', class_='itemPrice'):
    prices.append(j.text.strip())

prices.append(np.nan)
prices.append(np.nan)
df['item'] = items
df['price'] = prices


df.to_csv('output.csv')

    
    


