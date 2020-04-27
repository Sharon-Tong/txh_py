# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/21 15:44 
'''

import pandas as pd
from collections import defaultdict
class GetEveryRtlQty():

    #####################################################################
    # 计算销量之和--GD
    #####################################################################
    def eachrtlqty(self, order_data,order_y):
        CustInfDt = order_data.drop_duplicates(['CITY_NAME_S','REGION_CODE_NAME','REGION_COM_NAME','CUST_CODE','CUSTM_NAME'])
        CustInfDt = CustInfDt[['CITY_NAME_S','REGION_CODE_NAME','REGION_COM_NAME','CUST_CODE','CUSTM_NAME']]
        order_data[[order_y]] = order_data[[order_y]].astype('float64')

        dictlist = defaultdict(list)
        for i in range(len(order_data)):
            key = order_data.loc[i, 'CUST_CODE']
            if key not in dictlist:
                dictlist[key] = order_data.loc[i,order_y]

            else:
                dictlist[key] = dictlist[key] + order_data.loc[i,order_y]


        sale_df = pd.DataFrame.from_dict(dictlist, orient='index', columns=['SUM_SALE_X'])
        sale_df = sale_df.reset_index().rename(columns={'index': 'CUST_CODE'})

        CustInfDt = pd.merge(CustInfDt,sale_df,how='left',on='CUST_CODE')

        return CustInfDt

    #####################################################################
    # 计算销量之和--非GD
    #####################################################################
    def Proveachrtlqty(self, order_data,order_y):

        order_data[[order_y]] = order_data[[order_y]].astype('float64')

        dictlist = defaultdict(list)
        for i in range(len(order_data)):
            key = order_data.loc[i, 'CUST_CODE']
            if key not in dictlist:
                dictlist[key] = order_data.loc[i,order_y]
            else:
                dictlist[key] = dictlist[key] + order_data.loc[i,order_y]

        sale_df = pd.DataFrame.from_dict(dictlist, orient='index', columns=['SUM_SALE_X'])
        sale_df = sale_df.reset_index().rename(columns={'index': 'CUST_CODE'})

        return sale_df




