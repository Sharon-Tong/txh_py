# -*- coding: utf-8 -*-

'''

 Time : 2020/3/30 15:12
 Author : SharonTong

'''
from GDZY_REPORT.T_gd_daily_sales_city_bar.config_for_daily_sales import SalesConfig
from GDZY_REPORT.excel.writeexcel import To_excel
import time
from GDZY_REPORT.T_gd_daily_sales_city_bar.daily_param import DailyParam

if __name__== "__main__":
    starttime=time.time()
    a=SalesConfig()
    a.get_sql()
    data=a.get_data()
    excel=To_excel(DailyParam)
    excel.save_data_2_excel(data)
    endtime=time.time()
    m,s=divmod(endtime-starttime, 60)
    print("运行时间为：%02d分%02d秒" % (m,s))
