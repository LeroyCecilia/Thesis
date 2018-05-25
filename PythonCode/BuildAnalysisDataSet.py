# -*- coding: utf-8 -*-
'''
@Description:
    经过前面两个阶段（1）Combine表的ETL处理（2）Combine表的数据清理 的处理，
    已经基于Combine构建了完整的分析数据集combineDetail。
@Author:Leroy
@Date:2018-05-18
'''

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
@Description: 处理processcombine中每一条记录，具体而言如下：
（1）接收以元组形式存在的记录；
（2）依据参数生成新的特征；
（3）将新的特征追加到文本文件中；
@Param item tuple：从数据库表中查询出来的一条记录
@Param fileName String: 保存数据的文件名称
"""
def buildAnalysisDataSet(item,fileName):

    #将数据写入到文本文件中
    with open(fileName,'a') as file:
        for i in range(59):
            file.write(str(item[i]))
            file.write(",")
        bal_rank = item[57]
        ro_rank = item[58]
        ufm_rank = item[59] 
        if (bal_rank in [4,5]) or (ro_rank in [4,5]) or (ufm_rank in [4,5]):
            checkResult = "fail"
            resultCode = 0
        else:
            checkResult = "pass"
            resultCode = 1
        
        file.write(str(checkResult))
        file.write(",")
        file.write(str(resultCode))
        file.write("\n")
        
#主函数
def main(sourceTableName):
    conn = hiveConn("10.141.212.26",10000,"root","linglong")
    #创建Cursor句柄
    cursor = conn.cursor()
    #操作
    sql = "select * from " + sourceTableName
    cursor.execute(sql)
    fileName = "analysisDataSet.txt"
    while True:
        item = cursor.fetchone()
        if item is None:
            break
        else:            
            buildAnalysisDataSet(item,fileName)
    #关闭cursor
    cursor.close()

main("combine_detail")