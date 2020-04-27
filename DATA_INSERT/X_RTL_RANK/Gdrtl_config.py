# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/21 23:00 
'''

class rtlrankparam():

    tab_for_gd  =  'GDZY.GD_RTL_RANK'

    col_list    = ['CITY_NAME_S','REGION_CODE_NAME','REGION_COM_NAME','CUST_CODE','CUSTM_NAME','SUM_SALE_X', \
                   'SALE_RANK','CUST_LEVEL','CUST_NAME', 'CUST_ADDRESS', 'SELL_TYPE_CODE', 'SELL_TYPE_NAME','TOP_N']

    tab_for_Provc = 'gdzy.P_former_N_tab'
    col_list_prove = ['CITY_CODE','CUST_CODE','ORDER_AMT','ORDER_QTY','SUM_SALE_X','SALE_RANK','CUST_NAME','CUST_LEVEL',\
                   'CUST_ADDRESS', 'SELL_TYPE_CODE', 'SELL_TYPE_NAME','CITY_NAME_S','REGION_CODE_NAME','FORMER_N']

    # ['CUST_CODE', 'SUM_SALE_X', 'REGION_CODE_S', 'CUST_NAME', 'CUST_ADDRESS', 'SELL_TYPE_CODE', 'SELL_TYPE_NAME',
    #  'REGION_NAME_S', 'REGION_COM_NAME', 'CITY_NAME_S', '备选名单户数', 'SALE_RANK']
#