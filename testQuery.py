# -*- coding: utf-8 -*-
"""
Created on Sun Dec 07 09:29:21 2014

@author: spacegoing
"""
import pymongo as mg
client=mg.MongoClient()
db=client['test']
colADayR=db['ADayR']
colf_a=db['f_a']
colAlpha=db['Alpha']
colf_aC=db['crackf_a']
colAlphaC=db['crackAlpha']
#%%
data=list[colADayR.find({})]