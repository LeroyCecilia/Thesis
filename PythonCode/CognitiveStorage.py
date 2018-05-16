# -*- coding: utf-8 -*-
'''
Comments here
This is a script which compute the value of data.
The format of the input DataFrame is as follows:
 Serial  Size   Replicas  CreateDate UpdateDate AccessDate    UserName   Group
 ---------------------------------------------------------------------------------
   1      10        3     2018.01.01 2018.01.01  2018.05.06    root      root
   2      20        3     2018.01.28 2018.01.28  2018.05.10    root      hdfs
   3      15        2     2018.02.09 2018.02.09  2018.05.15    leroy     hdfs
   
 Serial  Acl    AccessFrequency  Importance SecurityLevel  Value  ValueDensity
 ---------------------------------------------------------------------------------
   1  rwxr--r--       5               10         9           90        9      
   2  rwxrw-r--       2               8          5           80        4
   3  rwxrwxrwx       1               2          3           30        2        
   
'''

import numpy as np;
import pandas as pd;
from pandas import Series,DataFrame;
from datetime import datetime;

#Preprocess
def valueCompute(df):
    #计算创建时间、修改时间以及最近访问时间离当前日期的时间差
    for i in range(df["CreateDate"]):
        df.ix[i,"CreateDays"] = dateDiff(df.ix[i,"CreateDate"])
    for i in range(df["UpdateDate"]):
        df.ix[i,"UpdateDays"] = dateDiff(df.ix[i,"UpdateDate"])
    for i in range(df["AccessDate"]):
        df.ix[i,"AccessDays"] = dateDiff(df.ix[i,"AccessDate"])

    #计算ACL权限值并根据此计算User和Group的权限值
    for i in range(df["Acl"]):
        df.ix[i,"TotalAcl"],df.ix[i,"UserAcl"],df.ix[i,"GroupAcl"] = aclCount(df.ix[i,"Acl"])
    
    #将df中各个列的数据进行离差标准化，全部归一化到[0,1]区间内
    columnList = ["Size","Replicas","CreateDays","UpdateDays","AccessDays","TotalAcl",
                  "AccessFrequency","Importance","SecurityLevel","Value","ValueDensity",
                  "UserAcl","GroupAcl"]
    for item in columnList:
        maxValue = df[item].max()
        minValue = df[item].min()
        for i in range(df.shape[0]):
            df.ix[i,item] = (df.ix[i,item]-minValue)/(maxValue-minValue)
        
    
    #计算各个数据的价值
    for i in range(df.shape[0]):
        for item in columnList:
            df.ix[i,"Value"] += df.ix[i,item]

#计算时间差，返回结果为相差的天数
def dateDiff(item):
    #首先将item解析为一个个的字符串，分别表示年、月、日
    result = item.split(".")
    for i in range(len(result)):
        result[i] = int(result[i])
    originalDate = datetime(result[0],result[1],result[2]).timestamp()
    now = datetime.now().timestamp()
    daysDiff = int((now-originalDate)/(60*60*24))
    return daysDiff
    
#根据ACL权限计算其总权限值，并据此计算User和Group对应的权限值
def aclCount(acl):
    totalAclNum = 0;
    userAclNum = 0;
    groupAclNum = 0;
    for i in range(3):
        if acl[i]=="r":
            userAclNum += 4
            totalAclNum += 4
        elif acl[i]=="w":
            userAclNum += 2
            totalAclNum += 2
        elif acl[i]=="x":
            userAclNum += 1
            totalAclNum += 1
    for i in range(3,6):
        if acl[i]=="r":
            groupAclNum += 4
            totalAclNum += 4
        elif acl[i]=="w":
            groupAclNum += 2
            totalAclNum += 2
        elif acl[i]=="x":
            groupAclNum += 1
            totalAclNum += 1
    for i in range(6,9):
        if acl[i]=="r":
            totalAclNum +=4
        elif acl[i]=="w":
            totalAclNum +=2
        elif acl[i]=="x":
            totalAclNum+=1
    return totalAclNum,userAclNum,groupAclNum
    
    
    
    
        
    