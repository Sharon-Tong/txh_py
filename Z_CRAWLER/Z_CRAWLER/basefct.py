#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 28 11:02:06 2019
管控平台广东零售户指定规格的销售数据的基础方程集
数据库：广东25库

@author: yaochunling
"""

import sys
import requests
import re
import ast
import csv
from urllib import parse
import random
import time
import json
import ibm_db

class dataGatherBase():

    def __init__( self, configParam , conn, cur ):
        # 数据库连接
        self.conn = conn
        self.cur  = cur
    pass


    ##########################
    #爬虫主函数
    ##########################
    def crawl_date_bar_data(self, sales_date, bar_name ,sales_year, url, url2, headers, city_name ):

        print("爬虫的日期是："+ str(sales_year)+"-"+str(sales_date)+"；规格名称是：" + str(bar_name)+ "；城市是：" + str(city_name) )

        #每爬完一天一个城市的数据，就休眠一分多钟
        sj = random.randint(30, 60)
        time.sleep(sj)

        # 请求session信息
        s = requests.session()

        # 登录
        print("主页面链接：" + str(url))
        r = s.get(url, headers=headers, cert="mgy.pem")
        # 保存cookies
        c = requests.cookies.RequestsCookieJar()
        c.set("JSESSIONID", '623A4376786254782E1F22738D75836F')
        s.cookies.update(c)
        # 请求新链接
        da2 = sales_year + ','+ re.findall(r'.*-', sales_date, re.S)[0] + sales_year + ','+ re.search(r'-.*', sales_date, re.S)[0][1:]
        #print (da2)
        da3 = parse.quote(da2)

        sj = random.randint(5, 10)
        time.sleep(sj)

        bar_name_ba2 = parse.quote(bar_name)
        bar_name_ba2 = bar_name_ba2.replace('%28', '(')
        bar_name_ba2 = bar_name_ba2.replace('%29', ')')
        #print (bar_name_ba2)
        #print (bar_name)
        city_name_ba1 =parse.quote(city_name)

        #拼接新的链接
        url2_1 = url2+'cut=com_text_new%40default%3A'+city_name_ba1+'%7Citem_name%40default%3A' + bar_name_ba2+ '%7Cdate_sale%40ymd%3A' + da3
        print("爬取数据页面链接：" + str(url2_1))

        responts = s.get(url2_1, headers=headers, cert="pushDis.pem")

        #print (responts.text)

        print("爬虫的日期是：" + str(sales_year) + "-" + str(sales_date) + "；规格名称是：" + str(bar_name) + "；城市是：" + str(city_name)+ "，爬取已结束。")

        return responts.text



    ############################################
    #处理爬虫返回的数据
    ############################################
    def deal_responts_rec(self, data , tb_name ):

        #返回结果转换字典形式
        #print (data)
        data_rec = json.loads(str(data),strict=False)

        #判断返回数据有没有结果
        total_count = data_rec['total_cell_count']
        print("返回需要处理的数据长度为：" + str(total_count))

        #如果结果为0，则不继续执行数据处理；否则提取字段，并进行数据插入处理
        if total_count > 0:

            print("开始数据处理环节。。。。。。" )

            results_rec  = re.findall(r'"cells": \[(.*?)"total_cell_count',data,re.S)[0]
            results_rec1 = str(results_rec).replace(' ','').replace(']','').replace('[','')
            results_list = results_rec1.split('},')
            print (len(results_list))

            #处理数据并插入数据库
            PRE_SQL = "insert into " + tb_name + " (sales_date, co_num, com_text_new, organ_name, regie_name, cust_code,"\
                      " cust_name, cust_type, status_text, bar_code, item_name, qty_ord_t_sum, qty_ord_x_sum, qty_need_t_sum, "\
                      " qty_need_x_sum, line_amt_sum , update_time ) values "
            i_sql = " "
            count = 0
            for index, each_list in enumerate(results_list):

                #print (index)
                #构建字典形式
                list_rec = (str(each_list) + "\n}").replace('}\n,\n','')
                #print (list_rec)
                rec_list = json.loads(list_rec)

                #根据keyword找内容
                #日期
                sales_year    = self.strGet(rec_list, 'date_sale.year')
                sales_month   = self.strGet(rec_list, 'date_sale.month')
                sales_day     = self.strGet(rec_list, 'date_sale.day')
                sales_date    = repr(sales_year)+'-'+repr(sales_month)+'-'+repr(sales_day)

                co_num         = self.strGet(rec_list, 'co_num')
                com_text_new   = self.strGet(rec_list, 'com_text_new')
                organ_name     = self.strGet(rec_list, 'organ_name')
                regie_name     = self.strGet(rec_list, 'regie_name')
                cust_code      = self.strGet(rec_list, 'cust_code')
                cust_name      = self.strGet(rec_list, 'cust_name')
                cust_type      = self.strGet(rec_list, 'cust_type')
                status_text    = self.strGet(rec_list, 'status_text')
                bar_code       = self.strGet(rec_list, 'bar_code')
                item_name      = self.strGet(rec_list, 'item_name')
                qty_ord_t_sum  = self.strGet(rec_list, 'qty_ord_t_sum')
                qty_ord_x_sum  = self.strGet(rec_list, 'qty_ord_x_sum')
                qty_need_t_sum = self.strGet(rec_list, 'qty_need_t_sum')
                qty_need_x_sum = self.strGet(rec_list, 'qty_need_x_sum')
                line_amt_sum   = self.strGet(rec_list, 'line_amt_sum')

                i_sql = i_sql + " ('" + str(sales_date) + "', '" + str(co_num) + "', '" + str(com_text_new) + "', '" + str(organ_name) + "', '" \
                        + str(regie_name) + "', '" + str(cust_code) + "', '" + str(cust_name) + "', '"+ str(cust_type) + "', '" + str(status_text) + "','" + str(bar_code) + "','" + \
                        str(item_name) + "','" + str(qty_ord_t_sum) + "', '" + str(qty_ord_x_sum) + "', '" + str(qty_need_t_sum) + "', '" + str(qty_need_x_sum) + "', '" + \
                        str(line_amt_sum)+ "',"+ 'CURRENT_TIMESTAMP' + "),"
				

                if (index + 1) % 2000 == 0:
                    i_sql = i_sql.rstrip(',')
                    i_sql = PRE_SQL + i_sql
                    #print(i_sql)
                    ibm_db.exec_immediate(self.conn,i_sql)
                    ibm_db.commit(self.conn)
                    i_sql = ""
                    print("执行数据量为：" + str(index))


                count = count + 1

            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                #print(i_sql)
                ibm_db.exec_immediate(self.conn,i_sql)
                ibm_db.commit(self.conn)
            pass

            print("全部数据处理完毕，并已全部插入数据库，数量为：" + str(count))

        else:
            print("由于没有返回有效数据，则不需要进行数据处理。。。")

        return True



    ######################################
    # 字符串提取
    ######################################
    def strGet(self, jsonContent, key):
     
        if key in jsonContent:
            result = jsonContent[key]
        else:
            result = ''
        return result

