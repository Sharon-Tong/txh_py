# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 17:27 
'''

from GDZY_MODEL.GD_MODEL.LOG.MyLog import logger
from GDZY_MODEL.GD_MODEL.Sql_param.sqlconfig import sql_myself
from GDZY_MODEL.GD_MODEL.SQL_LINK.MySql_link import MysqlDB
from GDZY_MODEL.GD_MODEL.T_rfm_V1.rtl_rfm_base import RtlRfmScore
from GDZY_MODEL.GD_MODEL.T_rfm_V1.BASE.BaseData import dataGatherBase
from GDZY_MODEL.GD_MODEL.T_rfm_V1.rfm_config import RfmParam
import time
import pandas as pd

#########################################################
# 价值客户筛选评估分析模型
# 广东中烟传统的RFM模型
#########################################################
class RFMRtlKhGDZY():

    def __init__(self):
        self.Param = RfmParam
        self.func=RtlRfmScore()
        self.logger = logger.get_logger()
        # self.CC = MysqlDB(sql_myself) #
        # self.CC.connect()

    def GdzyRfm(self):
        self.logger.info("处理的核心户情况中烟为："+ str(RfmParam.corp_name) + "；处理的时间为：" + str(self.Param.busidate)+"；处理的地市为："+str(self.Param.city_name))

        # 原始数据的准备：规格 & 月份和星期的对应关系
        # 1.1 提取需要统计的规格：一、二类
        logger.get_logger().info('获取规格')
        bar_codes = dataGatherBase().sel_JL_bar_codes(self.Param.JL_vec,self.Param.corp_code,self.Param.brand_code)


        logger.get_logger().info('获取时间')
        weeks_month = dataGatherBase().sel_week_2_month(self.Param.busidate_vec)


        # 1.3 获取地市数据-原始数据
        logger.get_logger().info('获取原始数据')

        all_data = dataGatherBase().sel_rtl_M_data(self.Param.date_start, self.Param.date_end, self.Param.city_code)
        # all_data = self.CC.select("select * from gdzy.test")
        # all_data = pd.DataFrame(list(all_data),columns=['week_id','provc_code','city_code','bar_code','cust_code','order_amt','order_qty'])

        # 1.4 计算地市下每个规格的frm

        max_week_id = weeks_month[['week_id']].max()['week_id']

        # self.func.get_rtl_bar_score(allorderdata=all_data,current_week=max_week_id,w_customize=[1/3,1/3,1/3])

        # 1.5 计算品牌得分
        logger.get_logger().info('计算得分...')
        self.func.get_rtl_brand_score(allorderdata=all_data, current_week=max_week_id, w_customize=[1 / 3, 1 / 3, 1 / 3])

        dataGatherBase().ccon_close()

if __name__ == "__main__":
    start_time = time.time()
    bb= RFMRtlKhGDZY()
    bb.GdzyRfm()
    end_time = time.time()
    m,s = divmod(end_time-start_time,60)
    logger.get_logger().info('运行时间为：%3d分%2d秒' %(m,s))