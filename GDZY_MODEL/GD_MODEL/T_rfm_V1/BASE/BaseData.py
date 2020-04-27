# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 11:02 
'''


from GDZY_MODEL.GD_MODEL.SQL_LINK.MySql_link import MysqlDB
from GDZY_MODEL.GD_MODEL.SQL_LINK.Hive_link import MyHive
from GDZY_MODEL.GD_MODEL.Sql_param.sqlconfig import sql_myself,gd_hive
import pandas as pd

class dataGatherBase():

    def __init__(self ):

        self.hive_objData   = MyHive(gd_hive)
        self.db_objData     = MysqlDB(sql_myself)
        self.hive_objData.connect()
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
    def sel_rtl_M_data(self, date_start,date_end , city_code ):

        # s_sql = "select week_id, provc_code, city_code, bar_code, cust_code, order_amt, order_qty from gdzy_dm_yx.dd_order_w where p_week_id in " + \
        #          str(week_vec) +" and city_code ='"+str(city_code)+"' and order_qty>0 and bar_code in "+ str(bar_codes)

        s_sql="select week_id, provc_code, city_code, bar_code, cust_code, order_amt, order_qty from gdzy_dm_yx.dd_order_w "\
                + " where p_week_id >='" + date_start + "' and p_week_id<='" + date_end + \
                 "' and bar_code in (select bar_code from gdzy_dm_yx.dim_cigrt_all where brand_code='9999' and price_type_name  in ('一类','二类')" \
                "and prod_entp_code = '20440001')" + " and city_code = '" + city_code + "' and order_qty>0 "

        s_sql1 = "select week_id, provc_code, city_code, bar_code, cust_code, order_amt, order_qty from gdzy_dm_yx.dd_order_w where  order_qty>0 limit 100"

        data = self.hive_objData.select(s_sql)

        all_data = pd.DataFrame(list(data), columns=['week_id', 'provc_code','city_code','bar_code','cust_code','order_amt','order_qty'])
        return all_data

    def ccon_close(self):
        self.hive_objData.close()