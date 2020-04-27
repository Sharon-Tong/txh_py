# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/16 14:14
'''

# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 17:27 
'''

from GDZY_MODEL.GD_MODEL.LOG.MyLog import logger
from GDZY_MODEL.GD_MODEL.Sql_param.sqlconfig import db25_1
from GDZY_MODEL.GD_MODEL.SQL_LINK.DB2_link import MyDB2
from GDZY_MODEL.GD_MODEL.T_rfm_V2.rtl_rfm_class_base import RtlRfmMedianClass
from GDZY_MODEL.GD_MODEL.T_rfm_V2.BASE.BaseData import dataGatherBase
from GDZY_MODEL.GD_MODEL.T_rfm_V2.rfm_config import RfmParam
import time



#########################################################
# 价值客户筛选评估分析模型
# 广东中烟传统的RFM模型
#########################################################
class RFMRtlKhGDZY():

    def __init__(self):
        self.Param      = RfmParam
        self.func       = RtlRfmMedianClass()
        self.db_object  = MyDB2(db25_1)
        self.logger     = logger.get_logger()

        # self.CC = MysqlDB(sql_myself) #
        # self.CC.connect()

    def GdzyRfm(self):
        # 1.原始数据的准备：规格 & 月份和星期的对应关系
        #1.1 获取省份、地市
        city_list = dataGatherBase().get_city_code(self.Param.sql1)
        # print(self.Param.sql1)
        # print(city_list)
        # 1.2 提取需要统计的规格：一、二类
        logger.get_logger().info('获取规格...')
        bar_codes = dataGatherBase().sel_JL_bar_codes(self.Param.JL_vec, self.Param.corp_code, self.Param.brand_code)

        # 1.3 获取时间
        logger.get_logger().info('获取时间...')
        weeks_month = dataGatherBase().sel_week_2_month(self.Param.busidate_vec)

        #2.处理主函数：对地市进行循环
        for key,row in city_list.iterrows():
            self.logger.info("处理的核心户情况中烟为："+ str(RfmParam.corp_name) + "；处理的时间为：" + str(self.Param.busidate)+\
                             "；处理的省份为："+str(row['PROVC_NAME']) + "；处理的地市为："+str(row['CITY_NAME']))

            #2.1获取地市的零售户-原始数据
            logger.get_logger().info('获取原始数据...')
            all_data = dataGatherBase().sel_rtl_M_data(weeks_month, row['CITY_CODE'], bar_codes)
            # 2.2 中位数判断高低并插入数据库
            logger.get_logger().info('开始计算零售户的分类...')
            max_week_id = weeks_month[['week_id']].max()['week_id']
            self.func.get_rtl_class(all_data, max_week_id, self.Param.rfm_c_median, self.Param.fm_res_tb_struct)

            #2.3 当处理完一个地市，将地市临时表的flag 更新为1，表示该地市已处理
            self.db_object.connect()
            update_sql = "UPDATE DBREAD.TEMP_CITY_FLAG SET flag=1 WHERE CITY_CODE='" + row['CITY_CODE'] + "' "
            self.db_object.update(update_sql)
            print('%s地市已处理完毕，地市临时表的状态也已经更改...' % str(row['CITY_CODE']))

        self.db_object.close()



if __name__ == "__main__":
    start_time = time.time()
    bb= RFMRtlKhGDZY()
    bb.GdzyRfm()
    end_time = time.time()
    m,s = divmod(end_time-start_time,60)
    logger.get_logger().info('运行时间为：%3d分%2d秒' %(m,s))