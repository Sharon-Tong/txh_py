#!/usr/bin/python
# -*- coding: UTF-8 -*-
import requests
import re
import ast
from urllib import parse
import random
import time
import pyodbc
da0=['1,4-1,4','1,5-1,5','1,6-1,6','1,7-1,7','1,8-1,8','1,9-1,9','1,10-1,10','1,11-1,11','1,12-1,12','1,13-1,13','1,14-1,14','1,15-1,15','1,16-1,16','1,17-1,17','1,18-1,18','1,19-1,19',]
for da in da0:
    sj = random.randint(60, 100)
    time.sleep(sj)
    time_start=time.time()
    da=da
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
    da3=parse.quote(da2)
    print('查询日期为：',da2)
    city=['东莞','中山','云浮','佛山','广州','惠州','揭阳','梅州','汕头','汕尾','江门','河源','清远','湛江','潮州','珠海','肇庆','茂名','阳江','韶关']

    #链接数据库
    dsn="driver={IBM DB2 ODBC DRIVER};database=%s;hostname=%s;port=%s;protocol=tcpip;uid=gdzyuser;pwd=f16Ysj9B@gdzyuser;"%("GDZY2","10.160.19.25","50000")
    connStr = pyodbc.connect(dsn)
    cursor = connStr.cursor()

    for ci in city:
        sj = random.randint(5, 10)
        time.sleep(sj)
        ci2=parse.quote(ci)
        url2='https://data.gd.tobacco.com.cn:8443/gkpt/server/cube/MODEL_AN_SD_DD_001_CUBES_GY/aggregate?drilldown=date_sale%40ymd%3Aday%7Ccom_text_new%40default%3Acom_text_new%7Corgan_name%40default%3Aorgan_name%7Ccust_code%40default%3Acust_code%7Ccust_name%40default%3Acust_name%7Ccust_type%40default%3Acust_type&cut=com_text_new%40default%3A'+ci2+'%7Cdate_sale%40ymd%3A'+da3
        responts=s.get(url2, headers=headers,cert="mgy.pem")
        list=re.findall(r'"cells": \[(.*?)"total_cell_count',responts.text,re.S)[0]
        list=list.replace(' ','')
        list=list.replace('],','')
        list=list.replace('\n','')
        #判断是否有销售
        if list=='':
            print(ci,'本日无销售')
            continue
        list=list.replace('},{','},,,{')
        list=list.replace('\\\\','\\')
        #字符串转列表
        list=list.split(",,,")

        j = 0
        for d in list:
            j=j+1
            #列表转字典
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
            b8 = dict1['qty_need_t_sum']
            b9 = dict1['qty_need_x_sum']
            b10 = dict1['qty_ord_t_sum']
            b11 = dict1['qty_ord_x_sum']
            b12 = dict1['line_amt_sum']
            #写入数据库
            cursor.execute("insert into dbread.dim_custm_gd(date_id, city_name,region_com_name,custm_code,custm_name,custm_lever,demand_qty_t,demand_qty_x,sales_qty_t,sales_qty_x,sales_amt) values (?,?,?,?,?,?,?,?,?,?,?)",(b1, b2, b3,b4, b5, b6,b8, b9, b10,b11, b12))

        print('正在导入',ci,j,'行数据')

    #将修改动作进行提交
    connStr.commit()
    #查询
    cursor.execute('''select case when pd='0' then '                           无重复数据' else pd end pd from (
select distinct case when count>=2 then DATE_ID||'  '||CITY_NAME||'                           数据重复' else '0' end pd
from (select date_id,
             city_name,
             custm_code,
             count(DATE_ID) count
      from DBREAD.DIM_CUSTM_GD
      group by date_id, city_name, custm_code))''')
    row = cursor.fetchone()
    #将查询结果进行打印
    while row:
       (col1)= (row[0])
       row = cursor.fetchone()
       print(col1)
    #关闭游标
    cursor.close()
    #退出链接
    connStr.close()
    time_end=time.time()
    print('运行时间：',(time_end-time_start)/60)

