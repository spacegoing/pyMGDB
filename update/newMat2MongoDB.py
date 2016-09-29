# -*- coding: utf-8 -*-
"""
Created on Sun Oct 05 15:25:08 2014

@author: space_000
"""
from os import walk
from scipy.io import loadmat
import pymongo as mg

def upiter2(field=[],timeUnits=[[]],alag=[4,5,6,7],data=[],col=None): # update  (one stock-many dates) data
    for fie in xrange(len(field)):
        queryda={}
        for lag in alag:
            for t in xrange(len(timeUnits[0])):
                queryda['%s.%s.%s'%(field[fie],timeUnits[0,t][0],lag)]= \
                data[fie][0][t,lag-alag[0]][0].tolist()
        col.update({'_id':field[fie]},{'$set':queryda})

def finiter(field=[],ddate=0,alag=[4,5,6,7],col=None): # find per date's data
    queryda={'_id':0}
    for fie in xrange(len(field)):
        for lag in alag:
            queryda['%s.%s.%s'%(field[fie],ddate,lag)]= 1
    res=col.find({},queryda)
    return res

mypath='C:\Users\space_000\Documents\MATLAB\Data_CalcuedMF\\'
alag=[4,5,6,7]

client=mg.MongoClient()
db=client['2014MF']
col=db['ADayR']

# update .mat data in mypath
filenames=[]
for filename in walk(mypath).next()[2]:
    # Start reading documents
    datalo=loadmat(mypath+filename,variable_names=['ADayR','Fields','timeUnits'])
    data=datalo['ADayR']
    Fields=datalo['Fields']
    timeUnits=datalo['timeUnits']
    # exclude the suffix .SH or .SZ
    # Fields already don't have suffix
    field=[]
    for r in Fields:
            field.append(int(r[0][0]))
            # update
            upiter2(field,timeUnits,alag,data,col)