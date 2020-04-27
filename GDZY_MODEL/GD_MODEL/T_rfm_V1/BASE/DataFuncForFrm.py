# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 11:24 
'''

from GDZY_MODEL.GD_MODEL.LOG.MyLog import logger
from GDZY_MODEL.GD_MODEL.T_rfm_V1.rfm_config import RfmParam
from GDZY_MODEL.GD_MODEL.SQL_LINK.MySql_link import MysqlDB
from GDZY_MODEL.GD_MODEL.SQL_LINK.Hive_link import MyHive
from GDZY_MODEL.GD_MODEL.Sql_param.sqlconfig import sql_myself,gd_hive,sql163
import pandas as pd

class RfmDataFunct():

    #####################################################################
    #计算R、F、M
    #####################################################################
    def get_rtl_r_f_m(self,order_data,current_week):
        order_data[['order_qty']] = order_data[['order_qty']].astype('float64')
        all_rtl = order_data[['cust_code']]
        # 去重
        all_rtl_dup = all_rtl.drop_duplicates(subset=['cust_code'], inplace=False)
        order_data[['order_amt']]=order_data[['order_amt']].astype('float64')
        empty_df = pd.DataFrame()
        groups = order_data.groupby('cust_code')
        rtl_index = 0
        for each_cust_code in all_rtl_dup['cust_code']:
            # each_cust_code = all_rtl_dup.loc[rtl_index,'cust_code']
            # each_cust_dt   = order_data[order_data['cust_code']==each_cust_code]
            each_cust_dt = groups.get_group(each_cust_code)
            empty_df.loc[rtl_index,'month_id'] = RfmParam.busidate
            empty_df.loc[rtl_index,'provc_code'] = each_cust_dt['provc_code'].max()
            empty_df.loc[rtl_index,'city_code'] = each_cust_dt['city_code'].max()
            empty_df.loc[rtl_index,'cust_code'] = each_cust_code
            empty_df.loc[rtl_index,'bar_code'] = each_cust_dt['bar_code'].max()
            # empty_df['price_level'] = each_cust_dt[['price_level']].max()['price_level']
            ###开始计算每个零售户的RFMX
            # R：current_week与每个零售户下规格/品牌 的最近一次购买时间的比较
            empty_df.loc[rtl_index,'R_REGION'] = int(current_week) - each_cust_dt[['week_id']].max()['week_id']

            # F:频次，计算每个零售户购买的周数
            all_week = each_cust_dt[['week_id']]
            # 去重
            week_dup = all_week.drop_duplicates(subset=['week_id'], inplace=False)
            empty_df.loc[rtl_index,'F_REGION']  = len(week_dup)

            # M:金额，加总求和
            #empty_df.loc[rtl_index,'M_REGION']  = each_cust_dt[['order_amt']].sum()['order_amt']
            empty_df.loc[rtl_index,'M_REGION']  = each_cust_dt[['order_amt']].sum()['order_amt']
            # X:销量，加总求和
            empty_df.loc[rtl_index, 'X'] = each_cust_dt[['order_qty']].sum()['order_qty']

            rtl_index +=1

        return empty_df

    #####################################################################
    # 计算品牌的销量之和
    #####################################################################
    def get_brand_sale(self, order_data):
        order_data[['order_qty']] = order_data[['order_qty']].astype('float64')
        order_data[['order_amt']] = order_data[['order_amt']].astype('float64')
        dictlist = {}
        for i in range(len(order_data)):
            key = order_data.loc[i,'cust_code']
            if key not in dictlist:
                dictlist[key] = order_data.loc[i,'order_qty']
            else:
                dictlist[key] =  dictlist[key] + order_data.loc[i,'order_qty']
        dictlist1 = {}
        for i in range(len(order_data)):
            key = order_data.loc[i, 'cust_code']
            if key not in dictlist1:
                dictlist1[key] = order_data.loc[i, 'order_amt']
            else:
                dictlist1[key] = dictlist1[key] + order_data.loc[i, 'order_amt']

        sale_df = pd.DataFrame.from_dict(dictlist, orient='index', columns=['X'])
        sale_df = sale_df.reset_index().rename(columns={'index': 'cust_code'})

        money_df = pd.DataFrame.from_dict(dictlist1, orient='index', columns=['M_NUM'])
        money_df = money_df.reset_index().rename(columns={'index': 'cust_code'})

        #
        # all_rtl = order_data[['cust_code']]
        # order_data[['order_qty']] = order_data[['order_qty']].astype('float64')
        # # 去重
        # all_rtl_dup = all_rtl.drop_duplicates(subset=['cust_code'], inplace=False)
        # sale_df = pd.DataFrame()
        # for index, rtl_index in enumerate(all_rtl_dup.index):
        #     each_cust_code = all_rtl_dup.loc[rtl_index, 'cust_code']
        #     each_cust_dt = order_data[order_data['cust_code'] == each_cust_code]
        #     # X:销量，加总求和
        #     sale_df.loc[rtl_index, 'cust_code'] = each_cust_code
        #     sale_df.loc[rtl_index, 'X'] = each_cust_dt[['order_qty']].sum()['order_qty']
        return sale_df,money_df


    #####################################################################
    #数据归一化,最大最小值标准化
    #####################################################################
    def max_min_Standar(self,tNormData):

        #对R进行标准化
        max_r = tNormData[['R_REGION']].max()['R_REGION']
        min_r = tNormData[['R_REGION']].min()['R_REGION']
        if max_r - min_r > 0:
            tNormData['R'] = (max_r - tNormData[['R_REGION']])/(max_r-min_r)
        else:
            tNormData['R'] = 1

        #对F进行标准化
        max_f = tNormData[['F_REGION']].max()['F_REGION']
        min_f = tNormData[['F_REGION']].min()['F_REGION']
        if max_f - min_f > 0:
            tNormData['F'] = (tNormData[['F_REGION']]-min_f)/(max_f-min_f)
        else:
            tNormData['F'] = 1

        #对M进行标准化
        # print(tNormData.loc[:,'R_REGION','F_REGION','M_REGION'])
        max_m = tNormData[['M_REGION']].max()['M_REGION']
        min_m = tNormData[['M_REGION']].min()['M_REGION']

        if max_m - min_m > 0:
            tNormData['M'] = (tNormData[['M_REGION']]-min_m)/(max_m-min_m)
        else:
            tNormData['M'] = 1
        return tNormData

    #####################################################################
    #变异系数法
    #####################################################################
    def cv_weight(self,NormData):

        #统计值
        Statistic=NormData.describe()

        #离散系数
        Statistic.loc['cv'] = Statistic.loc['std'] / abs(Statistic.loc['mean'])

        Statistic.fillna(0.0, inplace=True)
        all_cv = Statistic.loc['cv',['R_REGION','F_REGION','M_REGION']].sum()
        logger.get_logger().info('变异系数总和：'+ str(all_cv))
        if all_cv>0:
            Statistic.loc['cv_w'] = Statistic.loc['cv']/ all_cv
        else :
            Statistic.loc['cv_w']=1/3
        # logger.get_logger().info('统计值为\n'+ str(Statistic)+'\n注：如果三个指标无差异，那么将cv权重赋值为平均权重')
        NormData['R_W'] = Statistic.loc['cv_w']['R_REGION']
        NormData['F_W'] = Statistic.loc['cv_w']['F_REGION']
        NormData['M_W'] = Statistic.loc['cv_w']['M_REGION']

        # logger.get_logger().info('R、F、M的权重是%f,%f,%f' %(NormData['R_W'],NormData['F_W'],NormData['M_W']))

        return NormData



