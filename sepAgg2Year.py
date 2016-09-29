# -*- coding: utf-8 -*-
"""
Created on Sun Nov 30 09:52:06 2014

@author: space_000
"""
#将整个数据库MKD分为按年保存的数据库sepMD
from MongoPy.mgWindTools import findStockDateData,findConsecuDate
import pymongo as mg
client=mg.MongoClient()
db=client['MKD']
colMKInit=db['marketInit']

stockCode=colMKInit.find({'_id':'2014strStockCode'}).next()['strStockCode']
stride=100
numF=range(0,len(stockCode),stride)

date=findConsecuDate(20131231,betime=20130101)

db=client['sepMD']
colSep2013=db['min2013']
colSep2014=db['min2014']
#%%
for i in numF:
    stock=stockCode[i:i+stride]
    data=findStockDateData(stock)
    for i in data:
        i['_id']=int(i.keys()[0])
        colSep2014.insert(i)
#%%
for i in numF[1:]:
    stock=stockCode[i:i+stride]
    data=findStockDateData(stock,date)
    for i in data:
        i['_id']=int(i.keys()[0])
        colSep2013.insert(i)