# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 11:24 
'''

from GDZY_MODEL.GD_MODEL.T_rfm_V2.rfm_config import RfmParam

import pandas as pd

class RfmDataFunct():

    #####################################################################
    #计算R、F、M
    #####################################################################
    def get_rtl_r_f_m(self,order_data,current_week):

        all_rtl = order_data[['cust_code']]
        # 去重
        all_rtl_dup = all_rtl.drop_duplicates(subset=['cust_code'], inplace=False)
        order_data[['order_amt']]=order_data[['order_amt']].astype('float64')
        #时间
        month = RfmParam.busidate
        provc_code = order_data['provc_code'].max()
        city_code  = order_data['city_code'].max()

        empty_df = pd.DataFrame()
        groups = order_data.groupby('cust_code')
        rtl_index = 0
        for each_cust_code in all_rtl_dup['cust_code']:

            each_cust_dt = groups.get_group(each_cust_code)
            empty_df.loc[rtl_index,'cust_code']  = each_cust_code
            # empty_df.loc[rtl_index, 'cust_level'] = each_cust_code['CUST_LEVEL'].max()
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

        return empty_df

    #####################################################################
    #数据归一化,最大最小值标准化
    #####################################################################
    def max_min_Standar(self, tNormData):
        tNormData[['R_NUM']] = tNormData[['R_NUM']].astype('float64')
        tNormData[['F_NUM']] = tNormData[['F_NUM']].astype('float64')
        tNormData[['M_NUM']] = tNormData[['M_NUM']].astype('float64')
        # 对R进行标准化
        max_r = tNormData[['R_NUM']].max()['R_NUM']
        min_r = tNormData[['R_NUM']].min()['R_NUM']
        if max_r - min_r > 0:
            tNormData['R'] = (max_r - tNormData[['R_NUM']]) / (max_r - min_r)
        else:
            tNormData['R'] = 1

        # 对F进行标准化
        max_f = tNormData[['F_NUM']].max()['F_NUM']
        min_f = tNormData[['F_NUM']].min()['F_NUM']
        if max_f - min_f > 0:
            tNormData['F'] = (tNormData[['F_NUM']] - min_f) / (max_f - min_f)
        else:
            tNormData['F'] = 1

        # 对M进行标准化
        # print(tNormData.loc[:,'R_NUM','F_NUM','M_NUM'])
        max_m = tNormData[['M_NUM']].max()['M_NUM']
        min_m = tNormData[['M_NUM']].min()['M_NUM']

        if max_m - min_m > 0:
            tNormData['M'] = (tNormData[['M_NUM']] - min_m) / (max_m - min_m)
        else:
            tNormData['M'] = 1
        return tNormData







