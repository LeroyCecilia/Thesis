# -*- coding: utf-8 -*-
"""
@Description:
    本脚本主要用于将c_c_board_detail，c_c_inner_detail,c_c_inner_press_detail，
    c_c_mould_detail,p_b_build_pro_info_detail,p_c_cur_pro_info_detail中的属性进行预处理，
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
@Param host String : 主机名
@Param port int : 端口号
@Param username String : 用户名
@Param database String ：想要使用的数据库名称

@Return 
"""

def hiveConn(host,port,username,database):
    conn = hive.Connection(host=host, port=port, username=username, database=database)
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
    sql = "select * from " + sourceTableName
    cursor.execute(sql)
    fileName = "board_detail.txt"
    while True:
        item = cursor.fetchone()
        if item is None:
            break
        else:
            boardProcess(item,fileName)
    #最后将文本文件中的数据load到Hive数据库表中
    sql = "load data local inpath \'" + fileName + "\' overwrite into table " + newTableName
    #cursor.execute(sql)
    #将timeDimension.txt文件中的数据load到Hive表中
    sql = "load data local inpath \'" + "timeDimension.txt" + "\' overwrite into table time"
    #cursor.execute(sql)
    #关闭cursor
    cursor.close()

"""
@Description: 处理processBoardDetail中每一条记录，具体而言如下：
（1）接收以元组形式存在的记录；
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
        file.write(str(result['board_temp_avg']))
        file.write(",")
        file.write(str(result['board_temp_max']))
        file.write(",")
        file.write(str(result['board_temp_min']))
        file.write(",")
        file.write(str(result['board_temp_diff']))
        file.write(",")
        file.write(str(result['board_temp_std']))
        file.write(",")
        file.write(str(result['board_time_length']))
        file.write(",")
        file.write(result['starttime'])
        file.write(",")
        file.write(result['endtime'])
        file.write("\n")
    #将starttime交给timeDiemension（）函数用于产生时间维度表
    timeDimension(result['bar_code'],starttime)
    



"""
@Description:本函数主要用于对原始c_c_inner_detail表进行数据ETL，得到新的模式。
@Description: 根据starttime生成硫化时间相关的时间维度表，并将其存储到文本文件中
@Param bar_code String: 将bar_code作为time_id
@Param time datetime： 时间，形式为2018-10-10 08:00:00
"""
def timeDimension(bar_code,time):
    result = {}
    result['bar_code'] = bar_code
    result['time'] = str(time)
    result['year'] = str(time.year)
    result['month'] = str(time.month)
    result['day'] = str(time.day)
    if time.month >6:
        result['half_year'] = '上半年'
    else:
        result['half_year'] = '下半年'
    if time.month in [1,2,3]:
        result['quarter'] = '1'
    elif time.month in [4,5,6]:
        result['quarter'] = '2'
    elif time.month in [7,8,9]:
        result['quarter'] = '3'
    else:
        result['quarter'] = '4'
    result['weekofyear'] = str(time.strftime("%W"))
    result['dayofweek'] = str(time.weekday())
    if time.hour in range(6,19,1):
        result['dayornight'] = 'day'
    else:
        result['dayornight'] = 'night'
    result['hour'] = str(time.hour)
    result['minute'] = str(time.minute)
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
def processInnerDetail(conn,sourceTableName,newTableName):
    #创建Cursor句柄
    cursor = conn.cursor()
    #操作
    sql = "select * from " + sourceTableName
    cursor.execute(sql)
    fileName = "inner_detail.txt"
    while True:
        item = cursor.fetchone()
        if item is None:
            break
        else:            
            innerProcess(item,fileName)
    #关闭cursor
    cursor.close()

"""
@Description: 处理processInnerDetail中每一条记录，具体而言如下：
（1）接收以元组形式存在的记录；
（2）依据参数生成新的特征；
（3）将新的特征追加到文本文件中；
@Param item tuple：从数据库表中查询出来的一条记录
@Param fileName String: 保存数据的文件名称
"""
def innerProcess(item,fileName):
    #将数据处理成dict方便后续取出
    result = {}
    result['bar_code'] = item[0]
    result['inner_tempe'] = item[1]
    result['time'] = item[2]
    result['starttime'] = item[3]
    result['endtime'] = item[4]
    #item[5]的值缺失，默认是以"null"表示的。
    result['add_timestamp'] = item[6]
    #处理inner_tempe，求avg，max，min，diff，std。
    tempList = result['inner_tempe'].split(',')
    for i in range(len(tempList)):
        tempList[i] = float(tempList[i])
    result['inner_tempe_avg'] = np.mean(tempList)
    result['inner_tempe_max'] = np.max(tempList)
    result['inner_tempe_min'] = np.min(tempList)
    result['inner_tempe_diff'] = result['inner_tempe_max'] - result['inner_tempe_min']
    result['inner_tempe_std'] = np.std(tempList)
    #处理时间参数，形成时长、时间维度等新的特征
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
    
    #计算inner_time_length
    result['inner_time_length'] = (endtime.timestamp()- starttime.timestamp())/60
    #将数据写入到文本文件中
    with open(fileName,'a') as file:
        file.write(result['bar_code'])
        file.write(",")
        file.write(str(result['inner_tempe_avg']))
        file.write(",")
        file.write(str(result['inner_tempe_max']))
        file.write(",")
        file.write(str(result['inner_tempe_min']))
        file.write(",")
        file.write(str(result['inner_tempe_diff']))
        file.write(",")
        file.write(str(result['inner_tempe_std']))
        file.write(",")
        file.write(str(result['inner_time_length']))
        file.write(",")
        file.write(result['starttime'])
        file.write(",")
        file.write(result['endtime'])
        file.write(",")
        file.write(result['add_timestamp'])
        file.write("\n")

"""
@Description:本函数主要用于对原始c_c_inner_press_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processInnerPressDetail(conn,sourceTableName,newTableName):
    #创建Cursor句柄
    cursor = conn.cursor()
    #操作
    sql = "select * from " + sourceTableName
    cursor.execute(sql)
    fileName = "inner_press_detail.txt"
    while True:
        item = cursor.fetchone()
        if item is None:
            break
        else:            
            innerPressProcess(item,fileName)
    #关闭cursor
    cursor.close()


"""
@Description: 处理processInnerPressDetail中每一条记录，具体而言如下：
（1）接收以元组形式存在的记录；
（2）依据参数生成新的特征；
（3）将新的特征追加到文本文件中；
@Param item tuple：从数据库表中查询出来的一条记录
@Param fileName String: 保存数据的文件名称
"""
def innerPressProcess(item,fileName):
    #将数据处理成dict方便后续取出
    result = {}
    result['bar_code'] = item[0]
    result['inner_press'] = item[1]
    result['time'] = item[2]
    result['starttime'] = item[3]
    result['endtime'] = item[4]
    #处理inner_press，求avg，max，min，diff，std。
    pressList = result['inner_press'].split(',')
    for i in range(len(pressList)):
        pressList[i] = float(pressList[i])
    result['inner_press_avg'] = np.mean(pressList)
    result['inner_press_max'] = np.max(pressList)
    result['inner_press_min'] = np.min(pressList)
    result['inner_press_diff'] = result['inner_press_max'] - result['inner_press_min']
    result['inner_press_std'] = np.std(pressList)
    #处理时间参数，形成时长、时间维度等新的特征
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
    
    #计算inner_time_length
    result['inner_press_time_length'] = (endtime.timestamp()- starttime.timestamp())/60
    #将数据写入到文本文件中
    with open(fileName,'a') as file:
        file.write(result['bar_code'])
        file.write(",")
        file.write(str(result['inner_press_avg']))
        file.write(",")
        file.write(str(result['inner_press_max']))
        file.write(",")
        file.write(str(result['inner_press_min']))
        file.write(",")
        file.write(str(result['inner_press_diff']))
        file.write(",")
        file.write(str(result['inner_press_std']))
        file.write(",")
        file.write(str(result['inner_press_time_length']))
        file.write(",")
        file.write(result['starttime'])
        file.write(",")
        file.write(result['endtime'])
        file.write("\n")


"""
@Description:本函数主要用于对原始c_c_mould_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processMouldDetail(conn,sourceTableName,newTableName):
    #创建Cursor句柄
    cursor = conn.cursor()
    #操作
    sql = "select * from " + sourceTableName
    cursor.execute(sql)
    fileName = "mould_detail.txt"
    while True:
        item = cursor.fetchone()
        if item is None:
            break
        else:            
            mouldProcess(item,fileName)
    #关闭cursor
    cursor.close()



"""
@Description: 处理processMouldDetail中每一条记录，具体而言如下：
（1）接收以元组形式存在的记录；
（2）依据参数生成新的特征；
（3）将新的特征追加到文本文件中；
@Param item tuple：从数据库表中查询出来的一条记录
@Param fileName String: 保存数据的文件名称
"""
def mouldProcess(item,fileName):
    #将数据处理成dict方便后续取出
    result = {}
    result['bar_code'] = item[0]
    result['mould_tempe'] = item[1]
    result['time'] = item[2]
    result['starttime'] = item[3]
    result['endtime'] = item[4]
    #处理inner_press，求avg，max，min，diff，std。
    tempList = result['mould_tempe'].split(',')
    for i in range(len(tempList)):
        tempList[i] = float(tempList[i])
    result['mould_tempe_avg'] = np.mean(tempList)
    result['mould_tempe_max'] = np.max(tempList)
    result['mould_tempe_min'] = np.min(tempList)
    result['mould_tempe_diff'] = result['mould_tempe_max'] - result['mould_tempe_min']
    result['mould_tempe_std'] = np.std(tempList)
    #处理时间参数，形成时长、时间维度等新的特征
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
    
    #计算inner_time_length
    result['mould_time_length'] = (endtime.timestamp()- starttime.timestamp())/60
    #将数据写入到文本文件中
    with open(fileName,'a') as file:
        file.write(result['bar_code'])
        file.write(",")
        file.write(str(result['mould_tempe_avg']))
        file.write(",")
        file.write(str(result['mould_tempe_max']))
        file.write(",")
        file.write(str(result['mould_tempe_min']))
        file.write(",")
        file.write(str(result['mould_tempe_diff']))
        file.write(",")
        file.write(str(result['mould_tempe_std']))
        file.write(",")
        file.write(str(result['mould_time_length']))
        file.write(",")
        file.write(result['starttime'])
        file.write(",")
        file.write(result['endtime'])
        file.write("\n")
        

"""
@Description:本函数主要用于对原始p_b_build_info_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processBuildInfoDetail(conn,sourceTableName,newTableName):
    #创建Cursor句柄
    cursor = conn.cursor()
    #操作
    sql = "select * from " + sourceTableName
    cursor.execute(sql)
    fileName = "build_info_detail.txt"
    while True:
        item = cursor.fetchone()
        if item is None:
            break
        else:            
            buildInfoProcess(item,fileName)
    #关闭cursor
    cursor.close()




"""
@Description: 处理processMouldDetail中每一条记录，具体而言如下：
（1）接收以元组形式存在的记录；
（2）依据参数生成新的特征；
（3）将新的特征追加到文本文件中；
@Param item tuple：从数据库表中查询出来的一条记录
@Param fileName String: 保存数据的文件名称
"""
def buildInfoProcess(item,fileName):
    #将数据处理成dict方便后续取出
    result = {}
    result['bar_code'] = item[0]
    result['plan_date'] = item[1]
    result['work_shop_code'] = item[2]
    result['shift_id'] = item[3]
    result['class_id'] = item[4]
    result['equip_code'] = item[5]
    result['zjs_id'] = item[6]
    result['batch_id'] = item[7]
    result['material_code'] = item[8]
    
    #将数据写入到文本文件中
    with open(fileName,'a') as file:
        file.write(result['bar_code'])
        file.write(",")
        file.write(str(result['plan_date'] ))
        file.write(",")
        file.write(str(result['work_shop_code']))
        file.write(",")
        file.write(str(result['shift_id']))
        file.write(",")
        file.write(str(result['class_id']))
        file.write(",")
        file.write(str(result['equip_code']))
        file.write(",")
        file.write(str(result['zjs_id']))
        file.write(",")
        file.write(result['batch_id'])
        file.write(",")
        file.write(result['material_code'])
        file.write("\n")
        


"""
@Description:本函数主要用于对原始p_c_cur_info_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processCurInfoDetail(conn,sourceTableName,newTableName):
    #创建Cursor句柄
    cursor = conn.cursor()
    #操作
    sql = "select * from " + sourceTableName
    cursor.execute(sql)
    fileName = "cur_info_detail.txt"
    while True:
        item = cursor.fetchone()
        if item is None:
            break
        else:            
            curInfoProcess(item,fileName)
    #关闭cursor
    cursor.close()


"""
@Description: 处理processMouldDetail中每一条记录，具体而言如下：
（1）接收以元组形式存在的记录；
（2）依据参数生成新的特征；
（3）将新的特征追加到文本文件中；
@Param item tuple：从数据库表中查询出来的一条记录
@Param fileName String: 保存数据的文件名称
"""
def curInfoProcess(item,fileName):
    #将数据处理成dict方便后续取出
    result = {}
    result['bar_code'] = item[0]
    result['work_shop_code'] = item[1]
    result['material_code'] = item[2]
    result['pot_id'] = item[3]
    result['shift_id'] = item[4]
    result['class_id'] = item[5]
    result['zjs_id'] = item[6]
    result['batch_id'] = item[7]
    result['starttime'] = item[8]
    result['endtime'] = item[9]
    result['typelevel'] = item[10]
    result['scan_time'] = item[11]
    result['mould_id'] = item[12]
    #将数据写入到文本文件中
    with open(fileName,'a') as file:
        file.write(result['bar_code'])
        file.write(",")
        file.write(str(result['work_shop_code']))
        file.write(",")
        file.write(str(result['material_code']))
        file.write(",")
        file.write(str(result['pot_id']))
        file.write(",")
        file.write(str(result['shift_id']))
        file.write(",")
        file.write(str(result['class_id']))
        file.write(",")
        file.write(str(result['zjs_id']))
        file.write(",")
        file.write(result['batch_id'])
        file.write(",")
        file.write(str(result['starttime']))
        file.write(",")
        file.write(str(result['endtime']))
        file.write(",")
        file.write(str(result['typelevel']))
        file.write(",")
        file.write(str(result['scan_time']))
        file.write(",")
        file.write(result['mould_id'])
        file.write("\n")
        


"""
@Description:本函数主要用于对原始r_q_qual_dph_detail表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processDphDetail(conn,sourceTableName,newTableName):
    #创建Cursor句柄
    cursor = conn.cursor()
    #操作
    sql = "select * from " + sourceTableName
    cursor.execute(sql)
    fileName = "dph_detail.txt"
    while True:
        item = cursor.fetchone()
        if item is None:
            break
        else:            
            dphProcess(item,fileName)
    #关闭cursor
    cursor.close()

"""
@Description: 处理processMouldDetail中每一条记录，具体而言如下：
（1）接收以元组形式存在的记录；
（2）依据参数生成新的特征；
（3）将新的特征追加到文本文件中；
@Param item tuple：从数据库表中查询出来的一条记录
@Param fileName String: 保存数据的文件名称
"""
def dphProcess(item,fileName):
    #将数据处理成dict方便后续取出
    result = {}
    result['dph_detail_id'] = item[0]
    result['equip_id'] = item[1]
    result['board_detail_id'] = item[0]
    result['inner_detail_id'] = item[0]
    result['inner_press_detail_id'] = item[0]
    result['mould_detail_id'] = item[0]
    result['build_info_detail_id'] = item[0]
    result['cur_info_detail_id'] = item[0]
    result['time_id'] = item[0]
    result['load'] = item[3]
    result['bal_rank'] = item[4]
    result['ro_rank'] = item[5]
    result['ufm_rank'] = item[6]
    result['djid'] = item[8]
    result['total_rank'] = item[9]
    result['up_low_rank'] = item[10]
    result['up_low_g'] = item[11]
    result['couple_rank'] = item[12]
    result['lower_rank'] = item[13]
    result['upper_rank'] = item[14]
    #将数据写入到文本文件中
    with open(fileName,'a') as file:
        file.write(result['dph_detail_id'])
        file.write(",")
        file.write(str(result['equip_id']))
        file.write(",")
        file.write(str(result['board_detail_id']))
        file.write(",")
        file.write(str(result['inner_detail_id']))
        file.write(",")
        file.write(str(result['inner_press_detail_id']))
        file.write(",")
        file.write(str(result['mould_detail_id']))
        file.write(",")
        file.write(str(result['build_info_detail_id']))
        file.write(",")
        file.write(result['cur_info_detail_id'])
        file.write(",")
        file.write(str(result['time_id']))
        file.write(",")
        file.write(str(result['load']))
        file.write(",")
        file.write(str(result['bal_rank']))
        file.write(",")
        file.write(str(result['ro_rank']))
        file.write(",")
        file.write(str(result['ufm_rank']))
        file.write(",")
        file.write(str(result['djid']))
        file.write(",")
        file.write(str(result['total_rank']))
        file.write(",")
        file.write(str(result['up_low_rank']))
        file.write(",")
        file.write(str(result['up_low_g']))
        file.write(",")
        file.write(str(result['couple_rank']))
        file.write(",")
        file.write(str(result['lower_rank']))
        file.write(",")
        file.write(str(result['upper_rank']))
        file.write("\n")
        

"""
@Description:本函数主要用于对原始combine表进行数据ETL，得到新的模式。
@Param  conn pyhive.hive.Connection 到Hive的连接实例
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processCombine(conn,sourceTableName,newTableName):
    #创建Cursor句柄
    cursor = conn.cursor()
    #操作
    sql = "select * from " + sourceTableName
    cursor.execute(sql)
    fileName = "combine_detail.txt"
    while True:
        item = cursor.fetchone()
        if item is None:
            break
        else:            
            combineProcess(item,fileName)
    #关闭cursor
    cursor.close()



"""
@Description: 处理processcombine中每一条记录，具体而言如下：
（1）接收以元组形式存在的记录；
（2）依据参数生成新的特征；
（3）将新的特征追加到文本文件中；
@Param item tuple：从数据库表中查询出来的一条记录
@Param fileName String: 保存数据的文件名称
"""
def combineProcess(item,fileName):
    #将数据处理成dict方便后续取出
    result = {}
    result['bar_code'] = item[0]
    result['dph_equip_id'] = item[1]
    result['dph_material_code'] = item[2]
    result['dph_bal_rank'] = item[3]
    result['dph_ro_rank'] = item[4]
    result['dph_ufm_rank'] = item[5]
    result['build_plan_date'] = item[7]
    result['build_work_shop_code'] = item[8]
    result['build_shift_id'] = item[9]
    result['build_class_id'] = item[10]
    result['build_equip_code'] = item[11]
    result['build_zjs_id'] = item[12]
    result['build_batch_id'] = item[13]
    result['build_material_code'] = item[14]
    result['cur_work_shop_code'] = item[15]
    result['cur_material_code'] = item[16]
    result['cur_pot_id'] = item[17]
    result['cur_shift_id'] = item[18]
    result['cur_class_id'] = item[19]
    result['cur_zjs_id'] = item[20]
    result['cur_batch_id'] = item[21]
    result['cur_tyrelevel'] = item[24]
    result['cur_scan_time'] = item[25]
    result['cur_mould_id'] = item[26]
    if item[27] == "null":
        return
    tempList = item[27].split(",")
    for i in range(len(tempList)):
        tempList[i] = float(tempList[i])
    result['board_temp_avg'] = np.mean(tempList)
    result['board_temp_max'] = np.max(tempList)
    result['board_temp_min'] = np.min(tempList)
    result['board_temp_diff'] = result['board_temp_max']-result['board_temp_min']
    result['board_temp_std'] = np.std(tempList)
    result['board_time_length'] = timeDiff(item[29],item[30])
    #根据board_starttime计算硫化时间指标
    starttime = item[29].split(" ")
    date = starttime[0].split("-")
    time = starttime[1].split(":")
    year = int(date[0])
    month = int(date[1])
    day = int(date[2])
    hour = int(time[0])
    minute = int(time[1])
    starttime = datetime(year,month,day,hour,minute)
    result['time'] = str(starttime)
    result['year'] = str(starttime.year)
    result['month'] = str(starttime.month)
    result['day'] = str(starttime.day)
    if starttime.month >6:
        result['half_year'] = '上半年'
    else:
        result['half_year'] = '下半年'
    if starttime.month in [1,2,3]:
        result['quarter'] = '1'
    elif starttime.month in [4,5,6]:
        result['quarter'] = '2'
    elif starttime.month in [7,8,9]:
        result['quarter'] = '3'
    else:
        result['quarter'] = '4'
    result['weekofyear'] = str(starttime.strftime("%W"))
    result['dayofweek'] = str(starttime.weekday())
    if starttime.hour in range(6,19,1):
        result['dayornight'] = 'day'
    else:
        result['dayornight'] = 'night'
    result['hour'] = str(starttime.hour)
    result['minute'] = str(starttime.minute)
    if item[31] == "null":
        return
    tempList = item[31].split(",")
    for i in range(len(tempList)):
        tempList[i] = float(tempList[i])   
    result['inner_tempe_avg'] = np.mean(tempList)
    result['inner_tempe_max'] = np.max(tempList)
    result['inner_tempe_min'] = np.min(tempList)
    result['inner_tempe_diff'] = result['inner_tempe_max']-result['inner_tempe_min']
    result['inner_tempe_std'] = np.std(tempList)
    result['inner_time_length'] = timeDiff(item[33],item[34])
    if item[36] == "null":
        return
    tempList = item[36].split(",")
    for i in range(len(tempList)):
        tempList[i] = float(tempList[i])   
    result['mould_tempe_avg'] = np.mean(tempList)
    result['mould_tempe_max'] = np.max(tempList)
    result['mould_tempe_min'] = np.min(tempList)
    result['mould_tempe_diff'] = result['mould_tempe_max']-result['mould_tempe_min']
    result['mould_tempe_std'] = np.std(tempList)
    result['mould_time_length'] = timeDiff(item[38],item[39])
    if item[40] == "null":
        return
    pressList = item[40].split(",")
    for i in range(len(pressList)):
        pressList[i] = float(pressList[i])   
    result['inner_press_avg'] = np.mean(pressList)
    result['inner_press_max'] = np.max(pressList)
    result['inner_press_min'] = np.min(pressList)
    result['inner_press_diff'] = result['inner_press_max']-result['inner_press_min']
    result['inner_press_std'] = np.std(pressList)
    result['inner_press_time_length'] = timeDiff(item[42],item[43])
    
    
    #将数据写入到文本文件中
    with open(fileName,'a') as file:
        file.write(result['bar_code'])
        file.write(",")
        file.write(str(result['board_temp_avg']))
        file.write(",")
        file.write(str(result['board_temp_max']))
        file.write(",")
        file.write(str(result['board_temp_min']))
        file.write(",")
        file.write(str(result['board_temp_diff']))
        file.write(",")
        file.write(str(result['board_temp_std']))
        file.write(",")
        file.write(str(result['board_time_length']))
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
        file.write(",")
        file.write(str(result['inner_tempe_avg']))
        file.write(",")
        file.write(str(result['inner_tempe_max']))
        file.write(",")
        file.write(str(result['inner_tempe_min']))
        file.write(",")
        file.write(str(result['inner_tempe_diff']))
        file.write(",")
        file.write(str(result['inner_tempe_std']))
        file.write(",")
        file.write(str(result['inner_time_length']))
        file.write(",")
        file.write(str(result['inner_press_avg']))
        file.write(",")
        file.write(str(result['inner_press_max']))
        file.write(",")
        file.write(str(result['inner_press_min']))
        file.write(",")
        file.write(str(result['inner_press_diff']))
        file.write(",")
        file.write(str(result['inner_press_std']))
        file.write(",")
        file.write(str(result['inner_press_time_length']))
        file.write(",")
        file.write(str(result['mould_tempe_avg']))
        file.write(",")
        file.write(str(result['mould_tempe_max']))
        file.write(",")
        file.write(str(result['mould_tempe_min']))
        file.write(",")
        file.write(str(result['mould_tempe_diff']))
        file.write(",")
        file.write(str(result['mould_tempe_std']))
        file.write(",")
        file.write(str(result['mould_time_length']))
        file.write(",")
        file.write(str(result['build_plan_date']))
        file.write(",")
        file.write(str(result['build_work_shop_code']))
        file.write(",")
        file.write(str(result['build_shift_id']))
        file.write(",")
        file.write(str(result['build_class_id']))
        file.write(",")
        file.write(str(result['build_equip_code']))
        file.write(",")
        file.write(str(result['build_zjs_id']))
        file.write(",")
        file.write(str(result['build_batch_id']))
        file.write(",")
        file.write(str(result['build_material_code']))
        file.write(",")
        file.write(str(result['cur_work_shop_code']))
        file.write(",")
        file.write(str(result['cur_material_code']))
        file.write(",")
        file.write(str(result['cur_pot_id']))
        file.write(",")
        file.write(str(result['cur_shift_id']))
        file.write(",")
        file.write(str(result['cur_class_id']))
        file.write(",")
        file.write(str(result['cur_zjs_id']))
        file.write(",")
        file.write(str(result['cur_batch_id']))
        file.write(",")
        file.write(str(result['cur_tyrelevel']))
        file.write(",")
        file.write(str(result['cur_scan_time']))
        file.write(",")
        file.write(str(result['cur_mould_id']))
        file.write(",")
        file.write(str(result['dph_equip_id']))
        file.write(",")
        file.write(str(result['dph_material_code']))
        file.write(",")
        file.write(str(result['dph_bal_rank']))
        file.write(",")
        file.write(str(result['dph_ro_rank']))
        file.write(",")
        file.write(str(result['dph_ufm_rank']))
        file.write("\n")
    

"""
@Description:用于计算时间差,返回分钟数
@Param starttime string ： 以206-10-10 09:00:00.0的形式存在
@Param endtime string ： 以206-10-10 09:00:00.0的形式存在
@Return 时间差 mins
"""
def timeDiff(starttime,endtime):
    starttime = starttime.split(" ")
    date = starttime[0].split("-")
    time = starttime[1].split(":")
    year = int(date[0])
    month = int(date[1])
    day = int(date[2])
    hour = int(time[0])
    minute = int(time[1])
    starttime = datetime(year,month,day,hour,minute)
    
    endtime = endtime.split(" ")
    date = endtime[0].split("-")
    time = endtime[1].split(":")
    year = int(date[0])
    month = int(date[1])
    day = int(date[2])
    hour = int(time[0])
    minute = int(time[1])
    endtime = datetime(year,month,day,hour,minute)
    #计算硫化时长
    return (endtime.timestamp()- starttime.timestamp())/60
    
#主函数，用于实施各个数据表的ETL
def ETL():

    conn = hiveConn("10.141.212.26",10000,"root","linglong")
    #processBoardDetail(conn,"c_c_board_detail","board_detail")
    #processInnerDetail(conn,"c_c_inner_detail","inner_detail")
    #processInnerPressDetail(conn,"c_c_inner_press_detail","inner_press_detail")
    #processMouldDetail(conn,"c_c_mould_detail","mould_detail")
    #processBuildInfoDetail(conn,"p_b_build_pro_info_detail","build_info_detail")
    #processCurInfoDetail(conn,"p_c_cur_pro_info_detail","cur_info_detail")
    #processDphDetail(conn,"r_q_qual_dph_info","dph_detail")
    processCombine(conn,"combine","combine_detail")
    conn.close()

#调用主函数
ETL()