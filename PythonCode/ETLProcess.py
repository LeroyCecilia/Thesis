# -*- coding: utf-8 -*-
"""
@Description:
    本脚本主要用于将c_c_board_detail，c_c_inner_detail,c_c_inner_press_detail，
    c_c_mould_detail,p_b_build_info_detail,p_c_cur_info_detail中的属性进行预处理，
    将复合特征拆分成新的特征。
    同时从r_q_qual_dph_detail表中抽取几个真正与质量结果有关的属性（对属性进行筛选）。
    对应生成：board_detail,inner_detail,inner_press_detail,mould_detail,
    build_info_detail,cur_info_detail以及dph_detail。
    最后针对combine表进行ETL处理得到combine_detail。
@Operation Steps:
    (1)通过pyhs2连接CDH中的Hive
    (2)针对各个表格中的数据进行逐行处理
    (3)保存成新的Hive数据表
    (4)关闭Hive连接
@Author: Leroy
@Date: 2018-05-18
"""

"""
@Description:本函数用于连接CDH中的Hive
@Param host String : 主机名
@Param port int : 端口号
@Param username String : 用户名
@Param database String ：想要使用的数据库名称

@Return 
"""

from pyhive import hive

def hiveConn(host,port,username,database):
    conn = hive.Connection(host=host, port=port, username=username, database=database)
    return conn
    


"""
@Description:本函数主要用于对原始的c_c_board_detail表进行ETL，得到新的模式。
@Param  conn : PyHive提供的用于连接HiveServer2的句柄
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processBoardDetail(conn,sourceTableName,newTableName):
    cursor = conn.cursor()
    cursor.execute('select * from c_c_board_detail limit 2')
    file = open('./hiveTemp.txt','w')
    while True:
        item = cursor.fetchone()
        if item is None:
            break;
        else:
            result = {}            
            result['bar_code'] = item[0]
            result['board_temp'] = item[1]
            result['time'] = item[2]
            result['starttime'] = item[3]
            result['endtime'] = item[4]
            resultString = board_detail_feature(result)
            #将数据写入到文本文件中，field terminated by ',',line terminated by '\n'
            file.write(resultString)
            print(result)
    cursor.close()
    conn.close()    
    #首先创建新的表
    sql = "create table IF NOT EXISTS board_detail()"
    cursor.execute(sql)
    pass


"""
@Description:该函数用于接收c_c_board_detail中的记录，并按照设计的样子得到新的记录（以字符串的形式）
@Param source dict: 原始数据字典
@Return resultString String: 返回的结果字符串
说明：
字符串的组成如下：
bar_code, board_temp_avg, board_temp_max,board_temp_min,board_temp_period"""
def board_detail_feature(source):
    resultString = ""
    resultString += source["bar_code"]
    tempList = source["board_temp"].split(",")
    for i in range(len(tempList)):
        tempList[i] = float(tempList[i])
    resultString += "\n"
    return resultString

"""
@Description:本函数主要用于对原始c_c_inner_detail表进行数据ETL，得到新的模式。
@Param  conn : PyHive提供的用于连接HiveServer2的句柄
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processInnerDetail(conn,sourceTableName,newTableName):
    pass

"""
@Description:本函数主要用于对原始c_c_inner_press_detail表进行数据ETL，得到新的模式。
@Param  conn : PyHive提供的用于连接HiveServer2的句柄
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processInnerPressDetail(conn,sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始c_c_mould_detail表进行数据ETL，得到新的模式。
@Param  conn : PyHive提供的用于连接HiveServer2的句柄
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processMouldDetail(conn,sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始p_b_build_info_detail表进行数据ETL，得到新的模式。
@Param  conn : PyHive提供的用于连接HiveServer2的句柄
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processBuildInfoDetail(conn,sourceTableName,newTableName):
    pass

"""
@Description:本函数主要用于对原始p_c_cur_info_detail表进行数据ETL，得到新的模式。
@Param  conn : PyHive提供的用于连接HiveServer2的句柄
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processCurInfoDetail(conn,sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始r_q_qual_dph_detail表进行数据ETL，得到新的模式。
@Param  conn : PyHive提供的用于连接HiveServer2的句柄
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processDphDetail(conn,sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始combine表进行数据ETL，得到新的模式。
@Param  conn : PyHive提供的用于连接HiveServer2的句柄
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processCombine(conn,sourceTableName,newTableName):
    pass


#主函数，用于实施各个数据表的ETL
def ETL():
    conn = hiveConn('10.141.212.26',10000,'root','linglong')
    """processBoardDetail(conn,"c_c_board_detail","board_detail")
    processInnerDetail(conn,"c_c_inner_detail","inner_detail")
    processInnerPressDetail(conn,"c_c_inner_press_detail","inner_press_detail")
    processMouldDetail(conn,"c_c_mould_detail","mould_detail")
    processBuildInfoDetail(conn,"p_b_build_info_detail","build_info_detail")
    processCurInfoDetail(conn,"p_c_cur_info_detail","cur_info_detail")
    processDphDetail(conn,"r_q_dph_info_detail","dph_detail")
    processCombine(conn,"combine","combine_detail")"""
    
ETL()