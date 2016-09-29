# -*- coding: utf-8 -*-
"""
Created on Sat Dec 06 21:34:58 2014

@author: Chang
"""
import pymongo as mg
from collections import OrderedDict
# Todo:统一数据库 MarketInitdatabasename MFdatabasename MKDdatabasename
#%%
def upMF(stock='',f_aRes=[],AlphaRes=[],f_a1Res=[],Alpha1Res=[],ADayRes=[]): # update per stock's data

    client=mg.MongoClient()
    db=client['test']

    colADayR=db['ADayR']

    colf_a=db['f_a']
    colAlpha=db['Alpha']

    colf_aC=db['crackf_a']
    colAlphaC=db['crackAlpha']

    queryADayR,queryf_a,queryAlpha,queryf_aC,queryAlphaC=({} for i in xrange(5))
    for a in ADayRes.keys():
        for d in ADayRes[a].keys():
            queryADayR['%s.%s.%s'%(stock,d,a)]= \
            ADayRes[a][d].tolist()

            queryf_a['%s.%s.%s'%(stock,d,a)]= \
            f_aRes[a][d].tolist()
            queryAlpha['%s.%s.%s'%(stock,d,a)]= \
            AlphaRes[a][d].tolist()

            queryf_aC['%s.%s.%s'%(stock,d,a)]= \
            f_a1Res[a][d].tolist()
            queryAlphaC['%s.%s.%s'%(stock,d,a)]= \
            Alpha1Res[a][d].tolist()

    stock=int(stock)
    colADayR.update({'_id':stock},{'$set':queryADayR})

    colf_a.update({'_id':stock},{'$set':queryf_a})
    colAlpha.update({'_id':stock},{'$set':queryAlpha})

    colf_aC.update({'_id':stock},{'$set':queryf_aC})
    colAlphaC.update({'_id':stock},{'$set':queryAlphaC})
#%%
def findMfData(stockCode=[],dateSet=[],AlNoDays=[4,5,6,7],collection=['ADayR']):
    """
    用于查询给定stockCode、给定dateSet、给定collection（指标）、给定滞后期的多重分心计算结果
    stockCode=colMKInit.find({'_id':'2014strStockCode'}).next()['strStockCode']
    rdata=findMfData(stockCode)
    
    Parameters
    ----
    stockCode : N X 1 N只股票列表，股票代码为str，默认2014年前100只股票
    dateSet : M X 1 M个日期列表 日期为int，默认2014年至今所有交易日
    collection : ['ADayR','Alpha','f_a','crackAlpha','crackf_A']
    AlNoDays : 多重分形滞后期

    Returns
    ----
    {股票1:{}， 股票2:{}， 股票3:{} X M（股票长度个）...}
    {日期1:{}，日期2:{}，日期3:{} X N（日期长度个）...]}
    {滞后期1:[N个数据]，滞后期2:[N个数据]...}
    如果不存在股票、股票的日期，则赋为{}

    Notes
    ----
    1.返回数据类型中，股票顺序随机，但股票中的日期使用OrderedDict，日期顺序从前往后
    2.如果 股票数*日期长度 过大(>16MB)，则MongoDB会报错DocumentTooLarge
      此时需要分批查询
    """
    client=mg.MongoClient()
    db=client['MKD']
    colMKInit=db['marketInit']
    db=client['test']
    colAdayR=db['ADayR']
    colAlpha=db['Alpha']
    colf_a=db['f_a']
    colcrackA=db['crackAlpha']
    colcrackF=db['crackf_a']

    if stockCode==[]:
        stockCode=colMKInit.find({'_id':'2014strStockCode'}).next()\
        ['strStockCode'][:100]
    if dateSet==[]:
        dateSet=[i for i in colMKInit.find({'_id':'tdays'}).next()\
        ['tdays'] if 20130101<i<20140101]

    disp={k+'.'+str(d):1 for k in stockCode for d in dateSet}
    disp['_id']=0


    dateSet=[str(i) for i in dateSet]

    def rdataProc(col):
        #生成固定顺序的字典
        rdata={i.keys()[0]:OrderedDict(sorted(i.values()[0].items(),key=lambda x:x[0]))\
        for i in col.find({},disp) if i}

        for r in rdata:
            for d in rdata[r]:
                [rdata[r][d].pop(j) for j in \
                set(rdata[r][d].keys())-set(str(i) for i in AlNoDays)]

        return rdata

    rawData={}
    if 'ADayR' in collection:
        rawData['ADayR']=rdataProc(colAdayR)

    if 'Alpha' in collection:
        rawData['Alpha']=rdataProc(colAlpha)

    if 'f_a' in collection:
        rawData['f_a']=rdataProc(colf_a)

    if 'crackAlpha' in collection:
        rawData['crackAlpha']=rdataProc(colcrackA)

    if 'crackf_a' in collection:
        rawData['crackf_a']=rdataProc(colcrackF)

    return rawData

#%%
def clearEmptyStockDate(rdata,stockCode,dateSet,collection=['ADayR']):
    '''
    清空多重分形数据库中 1.全为空值的股票 2.某股票全为空值的日期

    Notes
    ----
    只需在多重分形计算完成后运行一次，否则再运行则报错

    Parameters
    ----
    rdata : MFpy.MFmongtools.mfMongoTools.findMfData 所返回数据
    stockCode : MarketInit中str股票
    dateSet : MarketInit中日期
    collection : 需清理的collection ['ADayR','Alpha','f_a','crackAlpha','crackf_A']

    Returns
    ----
    None

    '''
    client=mg.MongoClient()
    db=client['MKD']
    if type(dateSet[0])==int:
        dateSet=[str(i) for i in dateSet]

    empStockDate={s:{} for s in stockCode}
    stockUnset=[]
    stoDateUnset=[]

    for c in collection:
        for s in stockCode:
            for d in dateSet:
                acount=0
                for a in rdata[c][s][d].keys():
                    if not rdata[c][s][d][a]:
                        acount+=1
                if acount==len(rdata[c][s][d].keys()):
                    empStockDate[s][d]=1
            if len(dateSet)==sum(empStockDate[s].values()):
                empStockDate[s]=0
                stockUnset.append(({"_id":int(s)},True))
            if empStockDate[s]:
                stoDateUnset.append(({"_id":int(s)},{"$unset":{s+'.'+d:1 for d in empStockDate[s]}}))

    for i in collection:
        col=db[i]
        [col.remove(*s) for s in stockUnset]
        [col.update(*d) for d in stoDateUnset]
















