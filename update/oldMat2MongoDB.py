# -*- coding: utf-8 -*-
"""
Created on Thu Sep 18 23:07:30 2014

@author: space_000
"""
from os import walk
from scipy.io import loadmat
import pymongo as mg

def upiter(field=[],ddate=0,alag=[4,5,6,7],data=[],col=None): # update per date's data
    for fie in xrange(len(field)):
        queryda={}
        for lag in alag:
            queryda['%s.%s.%s'%(field[fie],ddate,lag)]= \
            data[fie,lag-alag[0]][0].tolist()
        col.update({'_id':field[fie]},{'$set':queryda})

def finiter(field=[],ddate=0,alag=[4,5,6,7],col=None): # find per date's data
    queryda={'_id':0}
    for fie in xrange(len(field)):
        for lag in alag:
            queryda['%s.%s.%s'%(field[fie],ddate,lag)]= 1
    res=col.find({},queryda)
    return res

mypath='D:\Data_Calcued\\'
alag=[4,5,6,7]

client=mg.MongoClient()
db=client['2014MF']
col=db['ADayR']

# update .mat data in mypath
filenames=[]
for filename in walk(mypath).next()[2]: 

    # Start reading documents
    datalo=loadmat(mypath+filename,variable_names=['ADayR1','fField','i'])
    data=datalo['ADayR1']
    th=datalo['i'][0][0]
    rfield=datalo['fField'][0,th-1] #read ith date's fField 
    
    # exclude the suffix .SH or .SZ
    field=[]
    for r in rfield: 
        field.append(int(r[0][0][:6]))
    # update
    upiter(field,int(filename[0:4]+ filename[5:7]+\
    filename[8:10]),alag,data,col)

#filenames=[]
#for filename in walk(mypath).next()[2]: # Start reading documents
#    filenames.append( filename)
#filename=filenames[0]
#datalo=loadmat(mypath+filename,variable_names=['ADayR1','fField','i'])
#data=datalo['ADayR1']
#th=datalo['i'][0][0]
#rfield=datalo['fField'][0,th-1]
#field=[]
#for r in rfield: 
#    field.append(int(r[0][0][:6]))
#upiter(field,int(filename[0:4]+ filename[5:7]+ filename[8:10]),alag,data,col)
#a=finiter(field,int(filename[0:4]+ filename[5:7]+ filename[8:10]),alag,col)
#for i in a:
#    print i

#scoff
#lieu
#repository
#directory innate genre leverage occurrence trivial 

# concurrent substage interoperability paradigm villain recursively
# chassis arbitrarily granularity pollen routines span


