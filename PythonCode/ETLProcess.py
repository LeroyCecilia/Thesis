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
@Param 
@Return 
"""
def hiveConn():
    pass


"""
@Description:本函数主要用于对原始的c_c_board_detail表进行ETL，得到新的模式。
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processBoardDetail(sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始c_c_inner_detail表进行数据ETL，得到新的模式。
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processInnerDetail(sourceTableName,newTableName):
    pass

"""
@Description:本函数主要用于对原始c_c_inner_press_detail表进行数据ETL，得到新的模式。
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processInnerPressDetail(sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始c_c_mould_detail表进行数据ETL，得到新的模式。
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processMouldDetail(sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始p_b_build_info_detail表进行数据ETL，得到新的模式。
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processBuildInfoDetail(sourceTableName,newTableName):
    pass

"""
@Description:本函数主要用于对原始p_c_cur_info_detail表进行数据ETL，得到新的模式。
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processCurInfoDetail(sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始r_q_qual_dph_detail表进行数据ETL，得到新的模式。
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processDphDetail(sourceTableName,newTableName):
    pass


"""
@Description:本函数主要用于对原始combine表进行数据ETL，得到新的模式。
@Param  sourceTableName String ：待处理的源Hive数据库表名
@Param  newTableName String  ：处理之后的新建Hive数据库表名
"""
def processCombine(sourceTableName,newTableName):
    pass


#主函数，用于实施各个数据表的ETL
def ETL():
    processBoardDetail("c_c_board_detail","board_detail")
    processInnerDetail("c_c_inner_detail","inner_detail")
    processInnerPressDetail("c_c_inner_press_detail","inner_press_detail")
    processMouldDetail("c_c_mould_detail","mould_detail")
    processBuildInfoDetail("p_b_build_info_detail","build_info_detail")
    processCurInfoDetail("p_c_cur_info_detail","cur_info_detail")
    processDphDetail("r_q_dph_info_detail","dph_detail")
    processCombine("combine","combine_detail")