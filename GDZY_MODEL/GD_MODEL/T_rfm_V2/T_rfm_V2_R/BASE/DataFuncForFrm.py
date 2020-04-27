# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 11:24 
'''

from GDZY_MODEL.GD_MODEL.T_rfm_V2.rfm_config import RfmParam
from GDZY_MODEL.GD_MODEL.LOG.MyLog import logger
import pandas as pd

class RfmDataFunct():

    #####################################################################
    #计算R、F、M
    #####################################################################
    def get_rtl_r_f_m(self,order_data,current_week):
        # print(order_data)
        # order_data['order_qty'] = order_data['order_qty'].astype('float64')
        all_rtl = order_data[['cust_code']]
        # 去重
        all_rtl_dup = all_rtl.drop_duplicates(subset=['cust_code'], inplace=False)
        order_data[['order_amt']]=order_data[['order_amt']].astype('float64')
        #时间
        month = RfmParam.busidate
        provc_code = order_data['provc_code'].max()
        city_code  = order_data['city_code'].max()
        # bar_code   = order_data['bar_code'].max()

        empty_df = pd.DataFrame()
        groups = order_data.groupby('cust_code')
        rtl_index = 0
        for each_cust_code in all_rtl_dup['cust_code']:

            each_cust_dt = groups.get_group(each_cust_code)
            # empty_df.loc[rtl_index,'month_id']   = month
            # empty_df.loc[rtl_index,'provc_code'] = provc_code
            #each_cust_dt['provc_code'].max()
            # empty_df.loc[rtl_index,'city_code']  = city_code
            empty_df.loc[rtl_index,'cust_code']  = each_cust_code
            # empty_df.loc[rtl_index,'bar_code']   = bar_code
            ###开始计算每个零售户的RFMX
            # R：current_week与每个零售户下规格/品牌 的最近一次购买时间的比较
            empty_df.loc[rtl_index,'R_REGION'] = int(current_week) - each_cust_dt[['week_id']].max()['week_id']

            # F:频次，计算每个零售户购买的周数
            all_week = each_cust_dt[['week_id']]
            # 去重
            week_dup = all_week.drop_duplicates(subset=['week_id'], inplace=False)
            empty_df.loc[rtl_index,'F_REGION']  = len(week_dup)

            # M:金额，加总求和
            empty_df.loc[rtl_index,'M_REGION']  = each_cust_dt[['order_amt']].sum()['order_amt']

            rtl_index +=1

        empty_df['month_id'] = month
        empty_df['provc_code'] = provc_code
        empty_df['city_code'] = city_code
        # empty_df['bar_code'] = bar_code

        # print(empty_df)
        return empty_df
    pass
    #####################################################################
    #数据归一化,最大最小值标准化
    #####################################################################
    def max_min_Standar(self, tNormData,R,F,M,R_S,F_S,M_S):
        tNormData[[R]] = tNormData[[R]].astype('float64')
        tNormData[[F]] = tNormData[[F]].astype('float64')
        tNormData[[M]] = tNormData[[M]].astype('float64')
        # 对R进行标准化
        max_r = tNormData[[R]].max()[R]
        min_r = tNormData[[R]].min()[R]
        if max_r - min_r > 0:
            tNormData[R_S] = (max_r - tNormData[[R]]) / (max_r - min_r)
        else:
            tNormData[R_S] = 1

        # 对F进行标准化
        max_f = tNormData[[F]].max()[F]
        min_f = tNormData[[F]].min()[F]
        if max_f - min_f > 0:
            tNormData[F_S] = (tNormData[[F]] - min_f) / (max_f - min_f)
        else:
            tNormData[F_S] = 1

        # 对M进行标准化
        # print(tNormData.loc[:,'R_NUM','F_NUM','M_NUM'])
        max_m = tNormData[[M]].max()[M]
        min_m = tNormData[[M]].min()[M]

        if max_m - min_m > 0:
            tNormData[M_S] = (tNormData[[M]] - min_m) / (max_m - min_m)
        else:
            tNormData[M_S] = 1
        return tNormData
    pass
    ##########################################
    #计算得分
    ##########################################
    def CountRfmScore(self,data,score,rank,r,f,m,weight):
        data[score] = data[r] * weight[0] + data[f] * weight[1] + data[m] * weight[2]
        data[rank]  = data[score].rank(ascending=False, method='first')

        return data
    pass

    ##########################################
    #计算变异系数法
    ##########################################
    def GetCvWeight(self,data,r_region,f_region,m_region):
        statistic = data[[r_region,f_region,m_region]].describe()

        cv = statistic.loc['std']/abs(statistic.loc['mean'])

        cv_sum = cv.sum()
        logger.get_logger().info('变异系数总和：' + str(cv_sum))

        if cv_sum>0:
            cv_weight = cv/cv_sum
        else:
            cv_weight = [1/3,1/3,1/3]
        logger.get_logger().info('R的cv权重是：%f；F的cv权重是：%f；M的cv权重是：%f；',cv_weight[r_region],cv_weight[f_region],cv_weight[m_region])

        return cv_weight











