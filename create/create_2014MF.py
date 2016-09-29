# -*- coding: utf-8 -*-
"""
Created on Wed Sep 17 19:44:05 2014

@author: space_000
"""
import pymongo as mg

client=mg.MongoClient()
db=client['MKD']
col=db['marketInit']
dateSet=[ str(i) for i in col.find({'_id':'tdays'}).next()['tdays'] if 20121231<i<20140101]
stockCode=col.find({'_id':'2014strStockCode'}).next()['strStockCode']
#%%
db=client['test']
colADayR=db['ADayR']
colf_a=db['f_a']
colAlpha=db['Alpha']
colf_aC=db['crackf_a']
colAlphaC=db['crackAlpha']
#%%
lag=[4,5,6,7]
alag={}
for l in lag:
    alag[str(l)]=[]

data={}
for d in dateSet:
    data[d]= alag

indata=[]
for s in stockCode:
    indata.append({'_id':int(s),'%s' % (s):data})
#%%
colADayR.insert(indata)

colf_a.insert(indata)
colAlpha.insert(indata)

colf_aC.insert(indata)
colAlphaC.insert(indata)
