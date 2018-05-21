# -*- coding: utf-8 -*-
"""
@Description:
    本脚本主要用于对board_detail,inner_detail,inner_press_detail,mould_detail,
    build_info_detail,cur_info_detail,dph_detail以及combine_detail进行缺失值检验
    和处理（填补）。经过处理的数据依旧存放在对应的表中，并不生成新的数据副本。
@Operation Steps:
    (1)通过pyhs2连接CDH中的Hive
    (2)针对各个表格中的数据进行逐行处理
    (3)将处理过的数据重新存储到Hive对应的表中
    (4)关闭Hive连接
@Author: Leroy
@Date: 2018-05-18
"""

"""
@Description:
    对表中的数据进行缺失值的检测和处理（此处是指填补），填补的策略使用KNN思想。
    填补之后的数据依旧存放在原始的Hive数据库表中。
@Param  tableName  String  要进行数据清理的表名
"""
def missingValueCompletion(tableName):
    #Connect to Hive in CDH.
    #Get the corresponding table and start completing the missing values.
    #Release Hive connection.
    pass


def completeMissingValue():
    tableNameList = ["board_detail","inner_detail","inner_press_detail"
                     ,"mould_detail","build_info_detail","cur_info_detail"
                     ,"dph_info_detail","combine_detail"]
    for tableName in tableNameList:
        missingValueCompletion(tableName)