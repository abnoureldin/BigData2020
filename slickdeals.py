#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

r = requests.get("https://slickdeals.net/laptop-deals/")
text = r.text

soup = BeautifulSoup(text, "html.parser")

brand=[]
item=[]
price=[]
likes=[]

for x in soup.find_all('div', class_='fpItem'):
    b = x.find('a', class_="itemStore")   
    if b is not None:
       brands = b.text
    else:
       brands = "None"
    brand.append(brands)    
    item.append(x.find('a', class_="itemTitle").text)    
    p = x.find('div', class_="priceLine")
    if p is not None:
        prices = p.text
    else:
        prices = "None"
    price.append(prices.strip())    
    likes.append(x.find('span',class_="count").text)

likes.append(np.nan)
price.append(np.nan)
df = pd.DataFrame({"Brand":brand, "Item":item, "Price":price, "Likes":likes})
df.to_csv("output.csv", index=False, encoding="utf-8")

    
    


