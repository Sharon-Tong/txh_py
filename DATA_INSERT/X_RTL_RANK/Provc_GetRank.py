# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/21 23:13 
'''

import pandas as pd
from DATA_INSERT.LOG.MyLog import logger
from DATA_INSERT.X_RTL_RANK.BASE.BaseData import GetDataFromHive
from DATA_INSERT.X_RTL_RANK.BASE.basefunc import GetEveryRtlQty
from DATA_INSERT.X_RTL_RANK.BASE.CreatSql import LoanCreateSql
import time
from fuzzywuzzy import process
from DATA_INSERT.X_RTL_RANK.Gdrtl_config import rtlrankparam
from DATA_INSERT.SQL_LINK.MySql_link import MysqlDB
from DATA_INSERT.Sql_param.sqlconfig import sql_myself
import pandas as pd
class getrankrtl():
    def __init__(self):
        self.db_object = MysqlDB(sql_myself)
        self.db_object.connect()

    def get_data(self,filename,sheetname):
        # 1. 获取地市
        data = pd.read_excel(filename, sheet_name=sheetname)
        city_list = data['地市']
        city_list_dup = city_list.drop_duplicates(inplace=False)
        for city_name in city_list_dup:

            logger.get_logger().info('正在处理地市:'+ city_name)
            # # 1.1 获取地市的城市编码
            # logger.get_logger().info('获取地市的城市编码...')
            # city_codes = GetDataFromHive().GetCityInf(city_name)
            # # 1.2 获取该地市下零售户的订单数据
            # logger.get_logger().info('获取地市的订单数据...')
            # orderdt = GetDataFromHive().GetOtherProvOrderData(city_codes['CITY_CODE'][0])
            # # 1.3 获取零售户的基本信息
            # logger.get_logger().info('获取客户的基本信息...')
            # custinf = GetDataFromHive().GetRtlInf(city_codes['CITY_CODE'][0])
            # # 1.4 获取零售户的档位信息
            # # logger.get_logger().info('获取客户的档位信息...')
            # # custlev = GetDataFromHive().GetCustLevel(city_name)
            # # 1.5 计算每个区域下每个零售户的销量
            # logger.get_logger().info('计算每个零售户的销量...')
            # each_rtl_x = GetEveryRtlQty().Proveachrtlqty(orderdt,'ORDER_QTY')
            #
            # # 1.7 匹配档位
            # # logger.get_logger().info('匹配档位...')
            # # new_dt = pd.merge(each_rtl_x,custlev,how='left',on='CUST_CODE')

            # # 1.8 匹零售户信息
            # logger.get_logger().info('匹配业态...')
            # new_dt = pd.merge(each_rtl_x,custinf,how='left',on='CUST_CODE')

            # 1.9 从DIM_REGION找到匹配的区县
            logger.get_logger().info('匹配区县...')
            regiondt = GetDataFromHive().GetRtlRegion(city_name)

            new_dt_for_xlsx = data.loc[data['地市']==city_name]
            # print(new_dt_for_xlsx)
            # print(regiondt)
            # print(regiondt)
            # 匹配区县的中文名
            # logger.get_logger().info('匹配区县的中文名称...')
            # new_dt = pd.merge(new_dt,regiondt,how = 'left',on = 'REGION_CODE_S')

            #匹配最相似的一个
            logger.get_logger().info('在区县表中匹配相似度最高的区县...')
            print(new_dt_for_xlsx['区县'])
            ner_dataframe = pd.DataFrame()
            ner_dataframe['区县']=new_dt_for_xlsx['区县']
            ner_dataframe['备选名单户数'] = new_dt_for_xlsx['备选名单户数']
            for i in new_dt_for_xlsx['区县']:
                # print(new_dt_for_xlsx.loc[i,'区县'])
                ner_dataframe.loc[i,'new区县'] = process.extractOne(i,regiondt['REGION_NAME_S'])[0]
            # print(new_dt.columns.tolist())
            # print(new_dt[['CUST_CODE','REGION_COM_NAME','REGION_NAME_S']])
            #
            # print(new_dt_for_xlsx[['地市','区县','备选名单户数']])

        #     new_dt['CITY_NAME_S'] = city_name
        #
        #
        #     #匹配推荐前N的个数，根据地市和区县匹配
        #     new_dt = pd.merge(new_dt,new_dt_for_xlsx[['地市','区县','备选名单户数']],how='left', \
        #                       left_on=['CITY_NAME_S','REGION_COM_NAME'],right_on =['地市','区县']).drop(['地市','区县'],axis=1)
        #
        #     # 1.6 计算每个区域下每个零售户的销量排名
        #     logger.get_logger().info('获取每个零售户的排名...')
        #     new_dt['SALE_RANK'] = new_dt.groupby(['REGION_COM_NAME'])['SUM_SALE_X'].rank(ascending=False, method='first')
        #     # print(new_dt)
        #     print(new_dt.columns.tolist())
        #     #模糊匹配
        #     # final_dt = new_dt[new_dt['SALE_RANK']<=new_dt['备选名单户数']]
        #     # print(final_dt)
        #     #  1.10 插入本地数据库
        #     logger.get_logger().info('开始插入数据库...')
        #     s_sql, sql_tab = LoanCreateSql().load_and_createsql(new_dt, rtlrankparam().tab_for_Provc,rtlrankparam().col_list_prove)
        #
        #     count = 0
        #     sql = ''
        #     for sql_tab_i in sql_tab:
        #
        #         sql = sql + sql_tab_i
        #         count = count + 1
        #         if count % 2000 == 0:
        #             i_sql = s_sql + sql.rsplit(',', 1)[0]
        #             sql = ''
        #             self.db_object.select(i_sql)
        #             logger.get_logger().info('插入数据%d条' % count)
        #
        #     i_sql = s_sql + sql.rsplit(',', 1)[0]
        #     self.db_object.select(i_sql)
        #     logger.get_logger().info('全部数据已插入，共插入数据%d条' % count)
        # self.db_object.close()


if __name__ == "__main__":
    start_time = time.time()
    logger.get_logger().info('开始计算省外的区县前N名零售户...')
    a= getrankrtl()
    # a.get_data(r"C:\Users\Ctitc\Desktop\20200420-汇总-办事处核心终端维护区域选择 .xlsx",'各办事处核心终端维护区域选择')
    a.get_data(r"C:\Users\sharon_Tong\Desktop\20200420-汇总-办事处核心终端维护区域选择 .xlsx",'各办事处核心终端维护区域选择')
    end_time = time.time()
    m, s = divmod(end_time - start_time, 60)
    logger.get_logger().info('程序结束...\n运行时间为：%3d分%2d秒' % (m, s))

