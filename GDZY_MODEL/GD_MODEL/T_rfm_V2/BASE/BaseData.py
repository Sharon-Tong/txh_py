# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 11:02 
'''



from GDZY_MODEL.GD_MODEL.SQL_LINK.DB2_link import MyDB2
from GDZY_MODEL.GD_MODEL.SQL_LINK.Hive_link import MyHive
from GDZY_MODEL.GD_MODEL.Sql_param.sqlconfig import gd_hive,db25_1
import pandas as pd

class dataGatherBase():

    def __init__(self ):

        self.hive_objData   = MyHive(gd_hive)
        self.db_objData     = MyDB2(db25_1)
        self.hive_objData.connect()
        self.db_objData.connect()


    pass


    #############################################################################
    # 提取需要统计的规格情况：bar_codes
    #############################################################################
    def sel_JL_bar_codes(self, JL_vec, ent_code, brand_code):

        s_sql = "select bar_code, price_type_code from gdzy_dm_yx.dim_cigrt_all where prod_entp_code='"+str(ent_code)+\
                "' and brand_code='"+str(brand_code)+"' and price_type_code in "+str(JL_vec)

        bar_codes = self.hive_objData.select(s_sql)

        bar_codes = pd.DataFrame(list(bar_codes), columns=['bar_code', 'price_type_code'])
        return bar_codes



    #############################################################################
    # 提取需要的月份和星期对应关系
    #############################################################################
    def sel_week_2_month(self, busidate_vec ):

        s_sql = "select month_id, week_id from gdzy_dm_yx.dim_date where month_id in "+ str(busidate_vec)

        data = self.hive_objData.select( s_sql )
        datedt = pd.DataFrame(list(data), columns=['month_id', 'week_id'])
        return datedt



    #############################################################################
    # 提取零售户的销售额数据
    #############################################################################
    def sel_rtl_M_data(self, week_vec,city_code ,bar_codes):

        all_week = week_vec[['week_id']]
        # 去重
        all_week_dup = all_week.drop_duplicates(subset=['week_id'], inplace=False)

        week_list = ""
        for week in all_week_dup['week_id']:
            week_list = week_list + "'" + week + "'" + ','
        week_list = "(" + week_list.rsplit(',',1)[0] + ')'

        all_bar_code = bar_codes[['bar_code']]
        # 去重
        bar_code_dup = all_bar_code.drop_duplicates(subset=['bar_code'], inplace=False)

        bar_code_list= ""
        for bar_code in bar_code_dup['bar_code']:
            bar_code_list = bar_code_list + "'" + bar_code+ "'" +','
        bar_code_list = "(" + bar_code_list.rsplit(',',1)[0] + ')'

        s_sql = "select week_id, provc_code, city_code, bar_code, cust_code, order_amt, order_qty from gdzy_dm_yx.dd_order_w where p_week_id in " + \
                week_list +" and city_code ='"+str(city_code)+"' and order_qty>0 and bar_code in "+ bar_code_list

        # s_sql1 = "SELECT MONTH_ID, PROV_CODE, CITY_CODE, CUST_CODE, R_NUM, F_NUM, M_NUM, R_CLUSTER, F_CLUSTER, M_CLUSTER, " \
        #          "CUST_CLUSTER_CODE, CUST_VALUE, CUST_VALUE_RANK FROM DBREAD.RFM_CUSTM_VALUE where CITY_CODE='" + city_code+"'"

        # data = self.db_objData.select(s_sql)
        data = self.hive_objData.select(s_sql)

        # all_data = pd.DataFrame(list(data), columns=['MONTH_ID','PROV_CODE','CITY_CODE','CUST_CODE','R_NUM','F_NUM','M_NUM',
        #                                             'R_CLUSTER','F_CLUSTER','M_CLUSTER','CUST_CLUSTER_CODE','CUST_VALUE','CUST_VALUE_RANK'])
        all_data = pd.DataFrame(list(data), columns=['week_id', 'provc_code','city_code','bar_code','cust_code','order_amt','order_qty'])
        return all_data

    def get_city_code(self,sql):
        # sql = "select * from dbread.temp_city_flag where index_id=1 and flag=0"
        city_all = self.db_objData.select(sql)
        city_all = pd.DataFrame(list(city_all),columns=['INDEX_ID', 'PROVC_CODE', 'PROVC_NAME', 'CITY_CODE', 'CITY_NAME', 'FLAG'])
        return city_all
    def get_cust_level(self,city_name):
        sql = "SELECT CUST_CODE,CUST_LEVEL FROM (select  cust_code,cust_level,ROW_NUMBER()OVER(PARTITION BY " \
              "cust_code ORDER BY SALES_DATE desc) rak from DBREAD.GD_CUSTM_DD WHERE city_name='" + str(city_name) + "') a"\
                " WHERE a.rak=1"
            # "select DISTINCT cust_code,cust_level from DBREAD.GD_CUSTM_DD where city_name='" + str(city_name) + "' and sales_date='2020-04-18'"
        print(sql)
        cust_level= self.db_objData.select(sql)
        cust_level = pd.DataFrame(list(cust_level),columns=['CUST_CODE', 'CUST_LEVEL'])
        return cust_level

    def ccon_close(self):
        self.hive_objData.close()