# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 11:05
# 目录参数
'''

class RfmParam():

        # 结果数据表
        rfm_res_tb           = "gdzy.t_rfm_gdzy_v2"      #数据表
        rfm_score_cv         = "gdzy.t_rfm_gdzy_score_v2" #零售户分数_cv
        rfm_score_avg        = "gdzy.t_rfm_gdzy_score_avg_v2"  # 零售户分数_avg
        #结果表结构
        fm_res_tb_struct     = ['month_id', 'provc_code', 'city_code', 'cust_code', 'bar_code', 'R_REGION', 'F_REGION', \
                                'M_REGION', 'X', 'R_W', 'F_W', 'M_W', 'R', 'F', 'M', 'QTY_W', 'AMT_W', 'RFM_SCORE', 'BAR_SCORE_QTY', 'BAR_SCORE_AMT']

        rfm_score_cv_struct  = ['month_id', 'provc_code', 'city_code', 'cust_code', 'R_REGION', 'F_REGION', \
                                'M_REGION', 'X', 'R', 'F', 'M', 'R_W', 'F_W', 'M_W', 'BRAND_SCORE', 'ALL_BAR_SCORE_QTY',\
                                'ALL_BAR_SCORE_AMT', 'ALL_BAR_SCORE_QTY_RANK', 'ALL_BAR_SCORE_AMT_RANK', 'BRAND_SCORE_RANK']


        rfm_score_avg_struct = ['month_id', 'provc_code', 'city_code', 'cust_code', 'R_REGION', 'F_REGION', \
                                'M_REGION', 'X', 'R', 'F', 'M', 'R_W', 'F_W', 'M_W', 'BRAND_SCORE', 'ALL_BAR_SCORE_QTY',\
                                'ALL_BAR_SCORE_AMT', 'ALL_BAR_SCORE_QTY_RANK', 'ALL_BAR_SCORE_AMT_RANK', 'BRAND_SCORE_RANK']


        # 匹配比例
        MAX_RATIO = 1.5

        # 日期
        busidate = "201909"

        # 统计时间维度
        busidate_vec = "('201907','201908','201909')"

        # 需要统计的城市
        city_name = "韶关市"
        city_code = "11440201"
        city_name_1 = "汕头市"
        city_code_1 = "11440501"

        # 指定价类
        JL_vec = "('01','02')"

        # 中烟信息
        corp_code = "20440001"
        corp_name = "广东中烟工业有限责任公司"
        group_code = "gdzy_1"
        brand_code = "9999"

        # 日志路径
        logger_path = r"/GDZY_MODEL/GD_MODEL"

        #开始日期
        date_start = '201927'
        date_end   = '201940'



