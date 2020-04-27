# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 14:06 
'''

from GDZY_MODEL.GD_MODEL.T_rfm_V2.T_rfm_V2_R.BASE.DataFuncForFrm import RfmDataFunct
from GDZY_MODEL.GD_MODEL.T_rfm_V2.rfm_config import RfmParam
from GDZY_MODEL.GD_MODEL.LOG.MyLog import logger
# import pandas as pd
from GDZY_MODEL.GD_MODEL.T_rfm_V2.T_rfm_V2_R.BASE.CreatSql import LoanCreateSql
from GDZY_MODEL.GD_MODEL.SQL_LINK.DB2_link import MyDB2
from GDZY_MODEL.GD_MODEL.Sql_param.sqlconfig import db25_1
import copy

class RtlRfmMedianClass():

    def __init__(self):
        self.para       = RfmParam
        self.datafunc   = RfmDataFunct()
        self.db_object  = MyDB2(db25_1)
        self.logger     =logger.get_logger()
        self.db_object.connect()


    #####################################################################
    ##判断每个产品下零售户的分值的高低
    #####################################################################

    def get_rtl_class(self,allorderdata,current_week,tab_name,colist,weight_type='cv',type_of_class='50%'):
        '''
        :param allorderdata: 订单数据集
        :param current_week: 计算R的'当前'时间，用这个时间计算最近一次的购买间隔（周）
        :param type_of_class: 高低比较的类型，中位数-50%，均值-mean(来自统计变量statistic)
        :return:
        '''
        # 获取R、F、M
        allorderdata = self.datafunc.get_rtl_r_f_m(allorderdata, current_week)

        if len(allorderdata)==0:
            logger.get_logger().info('该地区无双喜一二类烟的零售户...')
            pass

        else:
            statistic    = allorderdata.loc[:, ['R_REGION', 'F_REGION', 'M_REGION']].describe()

            value = statistic.loc[type_of_class]

            self.logger.info('R的最小值：%f；F的最大值是：%f；M的最大值是:%f' %(statistic.loc['min','R_REGION'],\
                             statistic.loc['max','F_REGION'],statistic.loc['max','M_REGION']))

            self.logger.info('R的中位数为：%f；R的中位数为：%f；M的中位数为：%f' % (value['R_REGION'], value['F_REGION'], value['M_REGION']))

            #判断R、F、M的高低分类
            #当R的中位数是R的最小值时，高分类是小于等于中位数
            if value['R_REGION'] == statistic.loc['min','R_REGION']:
                R_WetherBiggerThan = allorderdata['R_REGION'] <= value['R_REGION']
            else:
                R_WetherBiggerThan = allorderdata['R_REGION'] < value['R_REGION']

            #当F的中位数是R的最大值时，高分类是大于等于中位数
            if value['F_REGION'] == statistic.loc['max','F_REGION']:
                F_WetherBiggerThan = allorderdata['F_REGION'] >= value['F_REGION']
            else:
                F_WetherBiggerThan = allorderdata['F_REGION'] > value['F_REGION']

            # M分类是大于中位数（主要是认为中位数不会出现在最大值）
            M_WetherBiggerThan = allorderdata['M_REGION'] > value['M_REGION']

            # 将True/False 转为 1/0
            R_WetherBiggerThan = R_WetherBiggerThan + 0
            F_WetherBiggerThan = F_WetherBiggerThan + 0
            M_WetherBiggerThan = M_WetherBiggerThan + 0

            # WetherBiggerThan = allorderdata.loc[:, ['R_REGION', 'F_REGION', 'M_REGION']] > value
            # WetherBiggerThan = WetherBiggerThan + 0

            allorderdata['R_C'] = R_WetherBiggerThan
            allorderdata['F_C'] = F_WetherBiggerThan
            allorderdata['M_C'] = M_WetherBiggerThan

            # print(allorderdata)
            # 将R、F、M值的分类结果合成总的分类
            for index in range(len(allorderdata)):
                allorderdata.loc[index, 'ALL_C'] = ''.join([str(i) for i in allorderdata.loc[index,['R_C','F_C','M_C']]])
            new = allorderdata[['R_C','F_C','M_C']].replace(0, '低')
            new = new[['R_C','F_C','M_C']].replace(1, '高')
            allorderdata['R_C'] = new['R_C']
            allorderdata['F_C'] = new['F_C']
            allorderdata['M_C'] = new['M_C']

            ##计算得分
            #1.计算压缩值
            allorderdata = self.datafunc.max_min_Standar(allorderdata,'R_REGION','F_REGION','M_REGION','R','F','M')
            #2.计算得分、排名
            if weight_type=='cv':
                weight = self.datafunc.GetCvWeight(allorderdata,'R_REGION','F_REGION','M_REGION')
            else :
                weight = [1/3,1/3,1/3]

            allorderdata = self.datafunc.CountRfmScore(allorderdata,'SCORE','SCORE_RANK','R','R','M',weight)
            # print(allorderdata.columns.tolist())
            new_dt = allorderdata.loc[:,['month_id','provc_code','city_code','cust_code','R_REGION', 'F_REGION', 'M_REGION',\
                                         'R_C', 'F_C', 'M_C', 'ALL_C','SCORE', 'SCORE_RANK']]
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
                    print('插入数据%d条' % count)

            i_sql = s_sql + sql.rsplit(',', 1)[0]
            self.db_object.update(i_sql)
            print('全部数据已插入，共插入数据%d条' % count)






















