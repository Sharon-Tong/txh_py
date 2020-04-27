#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 11:02:06 2019
管控平台广东零售户指定规格的销售数据
数据库：广东25库

@author: yaochunling
"""
from config_crawler2 import  busParam
from basefct import dataGatherBase
import datetime
#import MySQLdb.cursors;
import ibm_db

class executeFun():

    def __init__( self ):

        #配置文件修改日期
        self.configParam = {
            "sales_year"  : busParam.sales_year,
            "sales_date"  : busParam.sales_date,
            "city"        : busParam.city,
            "url"         : busParam.url,
            "url2"        : busParam.url2,
            "headers"     : busParam.headers,
            "bar_names"   : busParam.bar_names,
            "tb_name"     : busParam.tb_name
        }

        self.tablename = self.configParam['tb_name']

        print ("db2连接。。。。。")
        self.conn = ibm_db.connect("DATABASE=GDZY2;HOSTNAME=10.160.19.25;PORT=50000;PROTOCOL=TCPIP;UID=gdzyuser;PWD=f16Ysj9B@gdzyuser;AUTHENTICATION=SERVER;","", "")
        self.cur  = None

        # 实例化
        self.dasefct = dataGatherBase(self.configParam, self.conn, self.cur  )
    pass



    ############################################################
    #爬虫主函数
    ############################################################
    def main_fct(self):

        print("爬虫开始。。。。。。")

        #for each_bar in self.configParam['bar_names']:
        for each_bar in ['双喜(喜百年)']:

            for each_date in self.configParam['sales_date']:

                for each_city in self.configParam['city']:
                #for each_city in ['汕头', '汕尾', '江门', '河源', '清远', '湛江', '潮州', '珠海', '肇庆', '茂名','阳江', '韶关']:

                    #爬取数据函数
                    responts_rec = self.dasefct.crawl_date_bar_data(each_date, each_bar ,self.configParam['sales_year'], self.configParam['url'], self.configParam['url2'], self.configParam['headers'], each_city )

                    #处理页面返回的数据
                    result_rec   = self.dasefct.deal_responts_rec( responts_rec , self.tablename)


        #关闭数据库连接
        print("关闭数据库连接。。。。。")
        ibm_db.close(self.conn)



if __name__ == "__main__":

    Start_time = datetime.datetime.now()

    flow = executeFun();

    flow.main_fct()

    End_time = datetime.datetime.now()

    print('程序执行时间为:'+str(End_time-Start_time))

    print (u'程序结束....')