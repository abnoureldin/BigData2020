#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 13 13:47:35 2020

@author: ar
"""
f = open("/Users/ar/Downloads/Shakespeare.txt","r")
text = f.read()
words = text.split()
count = dict()
for w in words:
    if w in count:
        count[w] += 1
    else:
        count[w] = 1
        
print(count)

import json
with open("/Users/ar/Downloads/Output.txt", "w") as file:
    file.write(json.dumps(count))
