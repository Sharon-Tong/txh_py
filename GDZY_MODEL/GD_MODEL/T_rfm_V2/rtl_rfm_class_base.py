# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 14:06 
'''

from GDZY_MODEL.GD_MODEL.T_rfm_V2.BASE.DataFuncForFrm import RfmDataFunct
from GDZY_MODEL.GD_MODEL.T_rfm_V2.rfm_config import RfmParam
from GDZY_MODEL.GD_MODEL.LOG.MyLog import logger
# import pandas as pd
from GDZY_MODEL.GD_MODEL.T_rfm_V2.BASE.CreatSql import LoanCreateSql
from GDZY_MODEL.GD_MODEL.SQL_LINK.DB2_link import MyDB2
from GDZY_MODEL.GD_MODEL.SQL_LINK.MySql_link import MysqlDB
from GDZY_MODEL.GD_MODEL.Sql_param.sqlconfig import db25_1,sql_myself
import copy

class RtlRfmMedianClass():

    def __init__(self):
        self.para       = RfmParam
        self.datafunc   = RfmDataFunct()
        self.db_object  = MysqlDB(sql_myself)
        self.logger     =logger.get_logger()
        self.db_object.connect()


    #####################################################################
    ##判断每个产品下零售户的分值的高低
    #####################################################################

    def get_rtl_class(self,allorderdata,current_week,tab_name,colist,value_w=[1/3,1/3,1/3],type_of_class='50%'):
        '''
        :param allorderdata: 订单数据集
        :param current_week: 计算R的'当前'时间，用这个时间计算最近一次的购买间隔（周）
        :param type_of_class: 高低比较的类型，中位数-50%，均值-mean(来自统计变量statistic)
        :return:
        '''
        # 获取R、F、M
        # allorderdata = self.datafunc.get_rtl_r_f_m(allorderdata, current_week)


        if len(allorderdata)==0:
            logger.get_logger().info('该地区无双喜一二类烟的零售户...')
            pass

        else:
            # 获取R、F、M的压缩值
            allorderdata = self.datafunc.max_min_Standar(allorderdata)
            # 计算R、F、M的价值得分，权重以输入的方式给定
            allorderdata['CUST_VALUE']= (allorderdata['R']*value_w[0]+allorderdata['F']*value_w[1]+allorderdata['M']*value_w[2])*100
            #价值分排名
            allorderdata['CUST_VALUE_RANK'] = allorderdata['CUST_VALUE'].rank(ascending=False, method='first')

            # #计算分类
            # statistic    = allorderdata.loc[:, ['R_REGION', 'F_REGION', 'M_REGION']].describe()
            #
            # value = statistic.loc[type_of_class]
            # self.logger.info('R阈值为：%f；R的阈值为：%f；M的阈值为：%f' % (value['R_REGION'], value['F_REGION'], value['M_REGION']))
            #
            # WetherBiggerThan = allorderdata.loc[:, ['R_REGION', 'F_REGION', 'M_REGION']] > value
            # WetherBiggerThan = WetherBiggerThan + 0
            #
            # allorderdata['R_C'] = WetherBiggerThan['R_REGION']
            # allorderdata['F_C'] = WetherBiggerThan['F_REGION']
            # allorderdata['M_C'] = WetherBiggerThan['M_REGION']
            #
            # # print(allorderdata)
            # # 总的分类结果
            # for index in range(len(allorderdata)):
            #     allorderdata.loc[index, 'ALL_C'] = ''.join([str(i) for i in allorderdata.loc[index,['R_C','F_C','M_C']]])
            # new = allorderdata[['R_C','F_C','M_C']].replace(0, '低')
            # new = new[['R_C','F_C','M_C']].replace(1, '高')
            # allorderdata['R_C']=new['R_C']
            # allorderdata['F_C'] = new['F_C']
            # allorderdata['M_C'] = new['M_C']


            print(allorderdata.columns.tolist())
            s_sql,sql_tab = LoanCreateSql().load_and_createsql(allorderdata,tab_name,colist)
            # print(s_sql + sql_tab[0])
            count = 0
            sql = ''
            for sql_tab_i in sql_tab:

                sql = sql + sql_tab_i
                count = count + 1
                if count % 2000 == 0:
                    i_sql = s_sql + sql.rsplit(',', 1)[0]
                    sql = ''
                    self.db_object.update(i_sql)
                    logger.get_logger().info('插入数据%d条' % count)

            i_sql = s_sql + sql.rsplit(',', 1)[0]
            self.db_object.update(i_sql)
            logger.get_logger().info('全部数据已插入，共插入数据%d条' % count)






















