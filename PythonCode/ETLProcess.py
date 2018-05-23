# -*- coding: utf-8 -*-
"""
@Description:
    本脚本主要用于将c_c_board_detail，c_c_inner_detail,c_c_inner_press_detail，
    c_c_mould_detail,p_b_build_info_detail,p_c_cur_info_detail中的属性进行预处理，
    将复合特征拆分成新的特征。
    同时从r_q_qual_dph_info表中抽取几个真正与质量结果有关的属性（对属性进行筛选）。
    对应生成：board_detail,inner_detail,inner_press_detail,mould_detail,
    build_info_detail,cur_info_detail以及dph_detail。
    最后针对combine表进行ETL处理得到combine_detail。
@Operation Steps:
    (1)通过PyHive连接CDH中的Hive
    (2)针对各个表格中的数据进行逐行处理
    (3)保存成新的Hive数据表
    (4)关闭Hive连接
@Author: Leroy
@Date: 2018-05-18
"""

import numpy as np
from datetime import datetime
import time as tModule
from pyhive import hive
"""
@Description:本函数用于连接CDH中的Hive
@Param host String ： Hive的主机名
@Param port Int : Hive主机端口号
@Param username String： 用户名
@Param database String： 要连接的数据库名
@Return conn pyhive.hive.Connection：到Hive的连接实例
"""
def hiveConn(host,port,username,database):
    conn = hive.Connection(host=host,port=port,username=username,database=database)
    return conn


"""
@Description:本函数主要用于对原始的c_c_board_detail表进行ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processBoardDetail(conn,sourceTableName,newTableName):
    #创建Cursor句柄
    cursor = conn.cursor()
    #操作
    sql = "select * from " + sourceTableName + " limit 10000"
    cursor.execute(sql)
    fileName = "board_detail.txt"
    while True:
        item = cursor.fetchone()
        if item is None:
            break
        else:
             boardProcess(item,fileName)
    #最后将文本文件中的数据load到Hive数据库表中
    sql = "load into local inpath \'" + fileName + "\' overwrite into table " + newTableName
    cursor.execute(sql)
    #将timeDimension.txt文件中的数据load到Hive表中
    sql = "load into local inpath \'" + "timeDimension.txt" + "\' overwrite into table time"
    cursor.execute(sql)
    #关闭cursor
    cursor.close()

"""
@Description: 处理processBoardDetail中每一条记录，具体而言如下：
（1）接收以元组形式的记录；
（2）依据参数生成新的特征；
（3）将新的特征追加到文本文件中；
@Param item tuple：从数据库表中查询出来的一条记录
@Param fileName String: 保存数据的文件名称
"""
def boardProcess(item,fileName):
    #将数据转存成dict形式，后续在查询的时候效率更高
    result = {}
    result['bar_code'] = item[0]
    result['board_temp'] = item[1]
    result['time'] = item[2]
    result['starttime'] = item[3]
    result['endtime'] = item[4]
    #处理board_temp，求avg，max，min，diff，std。
    tempList = result['board_temp'].split(',')
    for i in range(len(tempList)):
        tempList[i] = float(tempList[i])
    result['board_temp_avg'] = np.mean(tempList)
    result['board_temp_max'] = np.max(tempList)
    result['board_temp_min'] = np.min(tempList)
    result['board_temp_diff'] = result['board_temp_max'] - result['board_temp_min']
    result['board_temp_std'] = np.std(tempList)
    #处理时间参数，形成硫化时长、时间维度等新的特征
    starttime = result['starttime'].split(" ")
    date = starttime[0].split("-")
    time = starttime[1].split(":")
    year = int(date[0])
    month = int(date[1])
    day = int(date[2])
    hour = int(time[0])
    minute = int(time[1])
    starttime = datetime(year,month,day,hour,minute)
    
    endtime = result['endtime'].split(" ")
    date = endtime[0].split("-")
    time = endtime[1].split(":")
    year = int(date[0])
    month = int(date[1])
    day = int(date[2])
    hour = int(time[0])
    minute = int(time[1])
    endtime = datetime(year,month,day,hour,minute)
    
    #计算硫化时长
    result['board_time_length'] = (endtime.timestamp()- starttime.timestamp())/60
    #将数据写入到文本文件中
    with open(fileName,'a') as file:
        file.write(result['bar_code'])
        file.write(",")
        file.write(result['board_temp_avg'])
        file.write(",")
        file.write(result['board_temp_max'])
        file.write(",")
        file.write(result['board_temp_min'])
        file.write(",")
        file.write(result['board_temp_diff'])
        file.write(",")
        file.write(result['board_temp_std'])
        file.write(",")
        file.write(result['board_time_length'])
        file.write(",")
        file.write(result['starttime'])
        file.write(",")
        file.write(result['endtime'])
        file.write("\n")
    #将starttime交给timeDiemension（）函数用于产生时间维度表
    timeDimension(result['bar_code'],starttime)
    

"""
@Description: 根据starttime生成硫化时间相关的时间维度表，并将其存储到文本文件中
@Param bar_code String: 将bar_code作为time_id
@Param time datetime： 时间，形式为2018-10-10 08:00:00
"""
def timeDimension(bar_code,time):
    result = {}
    result['bar_code'] = bar_code
    result['time'] = time
    result['year'] = time.year
    result['month'] = time.month
    result['day'] = time.day
    if time.month >6:
        result['half_year'] = '上半年'
    else:
        result['half_year'] = '下半年'
    if time.month in [1,2,3]:
        result['quarter'] = 1
    elif time.month in [4,5,6]:
        result['quarter'] = 2
    elif time.month in [7,8,9]:
        result['quarter'] = 3
    else:
        result['quarter'] = 4
    result['weekofyear'] = tModule.strftime("%W")
    result['dayofweek'] = time.weekday
    if time.hour in range(6,19,1):
        result['dayornight'] = 'day'
    else:
        result['dayornight'] = 'night'
    result['hour'] = time.hour
    result['minute'] = time.minute
    #将信息写入到文本文件中
    with open('./timeDimension.txt','a') as file:
        file.write(result['bar_code'])
        file.write(",")
        file.write(result['time'])
        file.write(",")
        file.write(result['year'])
        file.write(",")
        file.write(result['month'])
        file.write(",")
        file.write(result['day'])
        file.write(",")
        file.write(result['half_year'])
        file.write(",")
        file.write(result['quarter'])
        file.write(",")
        file.write(result['weekofyear'])
        file.write(",")
        file.write(result['dayofweek'])
        file.write(",")
        file.write(result['dayornight'])
        file.write(",")
        file.write(result['hour'])
        file.write(",")
        file.write(result['minute'])
        file.write("\n")
        
    
"""
@Description:本函数主要用于对原始c_c_inner_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processInnerDetail(sourceTableName,newTableName):
    pass

"""
@Description:本函数主要用于对原始c_c_inner_press_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processInnerPressDetail(sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始c_c_mould_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processMouldDetail(sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始p_b_build_info_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processBuildInfoDetail(sourceTableName,newTableName):
    pass

"""
@Description:本函数主要用于对原始p_c_cur_info_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processCurInfoDetail(sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始r_q_qual_dph_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processDphDetail(sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始combine表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processCombine(sourceTableName,newTableName):
    pass


#主函数，用于实施各个数据表的ETL
def ETL():
    conn = hiveConn("10.141.212.26",10000,"root","linglong")
    processBoardDetail(conn,"c_c_board_detail","board_detail")
    processInnerDetail(conn,"c_c_inner_detail","inner_detail")
    processInnerPressDetail(conn,"c_c_inner_press_detail","inner_press_detail")
    processMouldDetail(conn,"c_c_mould_detail","mould_detail")
    processBuildInfoDetail(conn,"p_b_build_info_detail","build_info_detail")
    processCurInfoDetail(conn,"p_c_cur_info_detail","cur_info_detail")
    processDphDetail(conn,"r_q_dph_info_detail","dph_detail")
    processCombine(conn,"combine","combine_detail")
    conn.close()

#调用主函数
ETL()