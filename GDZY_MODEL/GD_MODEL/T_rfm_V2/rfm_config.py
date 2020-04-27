# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 11:05
# 目录参数
'''

class RfmParam():

        # 结果数据表
        # rfm_c_avg           = "gdzy.t_rfm_gdzy_class_m_v2" #零售户分数_cv
        rfm_c_median        = "DBREAD.RFM_CUSTM_VALUE"  # 零售户分数_avg
        # #结果表结构
        fm_res_tb_struct     =  ['MONTH_ID','PROV_CODE','CITY_CODE','CUST_CODE','R_NUM','F_NUM','M_NUM','R_CLUSTER',\
                                 'F_CLUSTER','M_CLUSTER', 'CUST_CLUSTER_CODE']



        #获取城市sql
        sql = "select * from dbread.temp_city_flag where city_name in ('韶关市')"

        # 获取城市sql2
        sql1 = "select * from dbread.temp_city_flag where index_id=213 "

        # 匹配比例
        MAX_RATIO = 1.5

        # 日期
        busidate = "202003"

        # 统计时间维度
        busidate_vec = "('202001','202002','202003')"

        # 指定价类
        JL_vec = "('01','02')"

        # 中烟信息
        corp_code = "20440001"
        corp_name = "广东中烟工业有限责任公司"
        group_code = "gdzy_1"
        brand_code = "9999"

        #本地数据库的表

        mysql_tab = "gdzy.tem_rfm_2020"
        mysql_tab_list = ['MONTH_ID', 'PROV_CODE', 'CITY_CODE', 'CUST_CODE', 'R_NUM', 'F_NUM', 'M_NUM', 'R_CLUSTER', \
                          'F_CLUSTER', 'M_CLUSTER', 'CUST_CLUSTER_CODE', 'CUST_VALUE', 'CUST_VALUE_RANK', 'CUST_LEVEL', \
                          'R', 'F', 'M']








