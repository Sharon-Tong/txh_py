#!/usr/bin/python
# -*- coding: UTF-8 -*-
import requests
import re
import ast
import csv
#import pyodbc
from urllib import parse
import random
import time
da=input('请输入日期(格式应为：m,d-m,d)：')
#请求session信息
s=requests.session()

url='https://data.gd.tobacco.com.cn:8443/gkpt/third/industryCommerce/dataAnalysis.jsp'
headers = {
    'Accept-Ranges': 'bytes',
    'Content-Type': 'application/javascript;charset=utf-8',
    'ETag': 'W/"84345-1558328670000"',
    'Server': 'Apache-Coyote/1.1',
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
    'Referer': 'https://data.gd.tobacco.com.cn:8443/gkpt/view.jspx?cubeId=74'
}
#登录
r=s.get(url, headers=headers,cert="mgy.pem")
#保存cookies
c = requests.cookies.RequestsCookieJar()
c.set("JSESSIONID",'623A4376786254782E1F22738D75836F')
s.cookies.update(c)
#请求新链接
da2='2020,'+re.findall(r'.*-',da,re.S)[0]+'2020,'+re.search(r'-.*',da,re.S)[0][1:]
da3=da3=parse.quote(da2)
print(da3)

city=['东莞','中山','云浮','佛山','广州','惠州','揭阳','梅州','汕头','汕尾','江门','河源','清远','湛江','潮州','珠海','肇庆','茂名','阳江','韶关']
bar=['双喜(国喜细支)','双喜(花悦)']
#wj = open('xiaoshou.csv', 'w', encoding='utf-8-sig', newline='')
#csv_writer = csv.writer(wj)
# 3. 构建列表头
#csv_writer.writerow(["日期", "城市", "区县", "客户编码", "客户名称", "档位", "规格", "需求量条", "需求量箱", "销量条", "销量箱", "金额"])
for ci in city:
    for ba in bar:
        sj = random.randint(5, 10)
        time.sleep(sj)
        ci2=parse.quote(ci)
        ba2=parse.quote(ba)
        ba2=ba2.replace('%28','(')
        ba2=ba2.replace('%29',')')
        print(ba2)
        print(ci2)
        url2='https://data.gd.tobacco.com.cn:8443/gkpt/server/cube/MODEL_AN_SD_DD_001_CUBES_GY/aggregate?drilldown=date_sale%40ymd%3Aday%7Ccom_text_new%40default%3Acom_text_new%7Corgan_name%40default%3Aorgan_name%7Ccust_code%40default%3Acust_code%7Ccust_name%40default%3Acust_name%7Ccust_type%40default%3Acust_type%7Citem_name%40default%3Aitem_name&cut=com_text_new%40default%3A'+ci2+'%7Citem_name%40default%3A'+ba2+'%7Cdate_sale%40ymd%3A'+da3
        responts=s.get(url2, headers=headers,cert="mgy.pem")
        list=re.findall(r'"cells": \[(.*?)"total_cell_count',responts.text,re.S)[0]
        list=list.replace(' ','')
        list=list.replace('],','')
        list=list.replace('\n','')
        list=list.replace('},{','},,,{')
        list=list.replace('\\\\','\\')
        list=list.split(",,,")
        print(url2)


        def f(d):
            nl = []
            for _ in d.values():
                nl += f(_) if isinstance(_, dict) else [_]
            return nl

        for d in list:
            dict1=ast.literal_eval(d)
            a1=dict1['date_sale.year']
            a2 = dict1['date_sale.month']
            a3 = dict1['date_sale.day']
            b1=repr(a1)+'-'+repr(a2)+'-'+repr(a3)
            b2=dict1['com_text_new']
            b3 = dict1['organ_name']
            b4 = dict1['cust_code']
            b5 = dict1['cust_name']
            b6 = dict1['cust_type']
            b7 = dict1['item_name']
            b8 = dict1['qty_need_t_sum']
            b9 = dict1['qty_need_x_sum']
            b10 = dict1['qty_ord_t_sum']
            b11 = dict1['qty_ord_x_sum']
            b12 = dict1['line_amt_sum']

            list2=[b1]+[b2]+[b3]+[b4]+[b5]+[b6]+[b7]+[b8]+[b9]+[b10]+[b11]+[b12]
            print(list2)
            # 4. 写入csv文件内容
            #csv_writer.writerow(list2)

#wj.close()

