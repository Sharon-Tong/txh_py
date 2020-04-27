# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/20 22:26 
'''

from DATA_INSERT.SQL_LINK.Hive_link import MyHive
from DATA_INSERT.SQL_LINK.DB2_link import MyDB2
from DATA_INSERT.Sql_param.sqlconfig import gd_hive,sql_myself,db25_1
from DATA_INSERT.LOG.MyLog import logger
import pandas as pd

class GetDataFromHive():

    def __init__(self):
        self.hive_object  = MyHive(gd_hive)
        self.db_object    = MyDB2(db25_1)

        #关联数据库
        self.hive_object.connect()
        self.db_object.connect()

    def GetOrderData(self,city_name):

        sql = "select city_name_s,region_code_name,region_com_name,custm_code,custm_name,custm_lever,sales_qty_x" \
              " from gdzy_dm_yx.xxk_gd_custm  where city_name_s='" + str(city_name) + "'"

        orderdt = self.hive_object.select(sql)
        orderdt = pd.DataFrame(list(orderdt),columns=['CITY_NAME_S','REGION_CODE_NAME','REGION_COM_NAME','CUST_CODE',\
                                                      'CUSTM_NAME','CUSTM_LEVER','SALES_QTY_X'])
        return orderdt

    def GetOtherProvOrderData(self,city_code):

        sql = "select city_code, cust_code, order_amt, order_qty from gdzy_dm_yx.dd_order_M where year_id='2019' and p_month_id>='201901' " \
              " and p_month_id<='201912' and city_code ='" + str(city_code) + "' and order_qty>0 and bar_code in (select bar_code " \
             "from gdzy_dm_yx.dim_cigrt_all where price_type_code in (01,02) and prod_entp_code='20440001') "


        Provorderdt = self.hive_object.select(sql)
        Provorderdt = pd.DataFrame(list(Provorderdt),columns=['CITY_CODE','CUST_CODE','ORDER_AMT','ORDER_QTY'])
        return Provorderdt

    def GetCityInf(self,city_name):
        sql = "select city_code,CITY_NAME_S from gdzy_dm_yx.DIM_CITY where CITY_NAME_S ='" + str(city_name) +"'"
        citydt = self.hive_object.select(sql)
        citydt = pd.DataFrame(list(citydt),columns=['CITY_CODE','CITY_NAME_S'])
        return citydt
    def GetRtlRegion(self,city_name):
        sql = "select distinct region_code_fixed,REGION_NAME_S,REGION_COM_NAME from gdzy_dm_yx.dim_region where CITY_NAME_S= '" + str(city_name) + "'"

        regiondt = self.hive_object.select(sql)
        regiondt = pd.DataFrame(list(regiondt),columns=['REGION_CODE_S','REGION_NAME_S','REGION_COM_NAME'])
        return regiondt
    def GetCustLevel(self,city_name):
        sql ="SELECT CUST_CODE,CUST_LEVEL FROM (select  cust_code,cust_level,ROW_NUMBER()OVER(PARTITION BY " \
            "cust_code ORDER BY SALES_DATE desc) rak from DBREAD.GD_CUSTM_DD WHERE city_name='" + str(city_name) + "') a" \
             " WHERE a.rak=1"

        clevdt = self.db_object.select(sql)
        clevdt = pd.DataFrame(list(clevdt),columns=['CUST_CODE','CUST_LEVEL'])

        return clevdt

    def GetRtlInf(self,city_code):
        # sql = "select region_code_s,custm_code,custm_name,custm_address from gdzy_dm_yx.dim_custm where city_code ='" + str(city_code)+"'"
        sql = "select custm_code,REGION_CODE_S,CUSTM_NAME,custm_address,SELL_TYPE_CODE,case when SELL_TYPE_CODE='Z' then '食杂店' when SELL_TYPE_CODE='B' " \
              "then '便利店' when SELL_TYPE_CODE='S' then '超市' when SELL_TYPE_CODE='N' then '商场' when SELL_TYPE_CODE='Y' then '烟酒商店'" \
              " when SELL_TYPE_CODE='F' then '娱乐服务类' when SELL_TYPE_CODE='Q' then '其他' end as SELL_TYPE_NAME" \
              " FROM GDZY_DM_YX.DIM_CUSTM WHERE CITY_CODE='" + str(city_code)+"'"

        rtldt = self.hive_object.select(sql)
        # rtldt = pd.DataFrame(list(rtldt),columns=['REGION_CODE_S','CUSTM_CODE','CUSTM_NAME','CUSTM_ADDRESS'])
        rtldt = pd.DataFrame(list(rtldt),columns=['CUST_CODE','REGION_CODE_S','CUST_NAME','CUST_ADDRESS','SELL_TYPE_CODE','SELL_TYPE_NAME'])
        return rtldt

    def close(self):
        self.hive_object.close()
        self.db_object.close()


