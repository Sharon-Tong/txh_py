#!/usr/bin/python
# -*- coding: UTF-8 -*-
import requests
import re
import ast
from urllib import parse
import random
import time
import datetime
import ibm_db

date_add = [-7,-6,-5,-4,-3,-2,-1]
time_set = "14:44:00"
while True:
    time_now = time.strftime("%H:%M:%S", time.localtime())  # 刷新
    if time_now == time_set:  # 此处设置每天定时的时间
        time_start = time.time()
        # 请求session信息
        session_req= requests.session()
        url = 'https://data.gd.tobacco.com.cn:8443/gkpt/third/industryCommerce/dataAnalysis.jsp'
        headers = {
            'Accept-Ranges': 'bytes',
            'Content-Type': 'application/javascript;charset=utf-8',
            'ETag': 'W/"84345-1558328670000"',
            'Server': 'Apache-Coyote/1.1',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
            'Referer': 'https://data.gd.tobacco.com.cn:8443/gkpt/view.jspx?cubeId=74'
        }
        # 登录
        log_in = session_req.get(url, headers=headers, cert="mgy.pem")
        print('登录状态码',log_in)

        # 保存cookies
        cookies_save = requests.cookies.RequestsCookieJar()
        cookies_save.set("JSESSIONID", '623A4376786254782E1F22738D75836F')
        session_req.cookies.update(cookies_save)

        for date_i in date_add:
            # 昨天日期
            yesterday = datetime.date.today() + datetime.timedelta(date_i)
            date_yest = str(yesterday.month) + ',' + str(yesterday.day)+'-'+str(yesterday.month) + ',' + str(yesterday.day)
            date_yest_full = '2020,' + re.findall(r'.*-', date_yest, re.S)[0] + '2020,' + re.search(r'-.*', date_yest, re.S)[0][1:]
            date_yest_sql = '2020-' + str(yesterday.month) + '-' + str(yesterday.day)
            print('查询日期为：', date_yest_full)
            # 链接数据库
            #db2_link = "driver={IBM DB2 ODBC DRIVER};database=%s;hostname=%s;port=%s;protocol=tcpip;uid=gdzyuser;pwd=f16Ysj9B@gdzyuser;" % (
            #"GDZY2", "10.160.19.25", "50000")
            #connStr = ibm_db.connect(db2_link)
            #connStr = ibm_db.connect("DATABASE=GDZY2;HOSTNAME=10.160.19.25;PORT=50000;PROTOCOL=TCPIP;" \
            #                      "UID=gdzyuser;PWD=f16Ysj9B@gdzyuser;AUTHENTICATION=SERVER;", "", "")
            #cursor = connStr.cursor()
            #print('链接数据库成功')
            #sql='delete from DBREAD.DIM_CUSTM_GD where DATE_ID=\'%s\'' % date_yest_sql
            #cursor.execute(sql)
            #print(date_yest_sql,'的数据已删除')

            #对日期进行编码
            date_yest_encoding = parse.quote(date_yest_full)

            #请求新链接
            #city_all = ['东莞', '中山', '云浮', '佛山', '广州', '惠州', '揭阳', '梅州', '汕头', '汕尾', '江门', '河源', '清远', '湛江', '潮州', '珠海', '肇庆','茂名', '阳江', '韶关']
            city_all = ['前10个城市', '后10个城市']
            url_one = 'https://data.gd.tobacco.com.cn:8443/gkpt/server/cube/MODEL_AN_SD_DD_001_CUBES_GY/aggregate?drilldown=date_sale%40ymd%3Aday%7Ccom_text_new%40default%3Acom_text_new%7Corgan_name%40default%3Aorgan_name%7Ccust_code%40default%3Acust_code%7Ccust_name%40default%3Acust_name%7Ccust_type%40default%3Acust_type&cut=com_text_new%40default%3A%E6%BD%AE%E5%B7%9E%3B%E6%B2%B3%E6%BA%90%3B%E6%A2%85%E5%B7%9E%3B%E6%B1%95%E5%A4%B4%3B%E4%BD%9B%E5%B1%B1%3B%E5%B9%BF%E5%B7%9E%3B%E4%BA%91%E6%B5%AE%3B%E4%B8%AD%E5%B1%B1%3B%E6%8F%AD%E9%98%B3%3B%E6%B1%95%E5%B0%BE%7Cdate_sale%40ymd%3A' + date_yest_encoding
            url_two = 'https://data.gd.tobacco.com.cn:8443/gkpt/server/cube/MODEL_AN_SD_DD_001_CUBES_GY/aggregate?drilldown=date_sale%40ymd%3Aday%7Ccom_text_new%40default%3Acom_text_new%7Corgan_name%40default%3Aorgan_name%7Ccust_code%40default%3Acust_code%7Ccust_name%40default%3Acust_name%7Ccust_type%40default%3Acust_type&cut=com_text_new%40default%3A%E6%B8%85%E8%BF%9C%3B%E6%B9%9B%E6%B1%9F%3B%E7%8F%A0%E6%B5%B7%3B%E9%98%B3%E6%B1%9F%3B%E6%B1%9F%E9%97%A8%3B%E8%8C%82%E5%90%8D%3B%E4%B8%9C%E8%8E%9E%3B%E8%82%87%E5%BA%86%3B%E9%9F%B6%E5%85%B3%3B%E6%83%A0%E5%B7%9E%7Cdate_sale%40ymd%3A' + date_yest_encoding
            url_total = 0
            for city in city_all:
                print(city)
                url_total = url_total + 1
                if url_total == 1:
                    url2 = url_one
                elif url_total == 2:
                    url2 = url_two
                responts = session_req.get(url2, headers=headers, cert="mgy.pem")
                print('查询状态码',responts)
                list = re.findall(r'"cells": \[(.*?)"total_cell_count', responts.text, re.S)[0]
                list = list.replace(' ', '')
                list = list.replace('],', '')
                list = list.replace('\n', '')
                # 判断是否有销售
                if list == '':
                    print(city, '本日无销售')
                    continue
                list = list.replace('},{', '},,,{')
                list = list.replace('\\\\', '\\')
                # 字符串转列表
                list = list.split(",,,")
                print('查询成功，开始写入')
                j = 0
                for d in list:
                    j = j + 1
                    # 列表转字典
                    dict1 = ast.literal_eval(d)
                    a1 = dict1['date_sale.year']
                    a2 = dict1['date_sale.month']
                    a3 = dict1['date_sale.day']
                    b1 = repr(a1) + '-' + repr(a2) + '-' + repr(a3)
                    b2 = dict1['com_text_new']
                    b3 = dict1['organ_name']
                    b4 = dict1['cust_code']
                    b5 = dict1['cust_name']
                    b6 = dict1['cust_type']
                    b8 = dict1['qty_need_t_sum']
                    b9 = dict1['qty_need_x_sum']
                    b10 = dict1['qty_ord_t_sum']
                    b11 = dict1['qty_ord_x_sum']
                    b12 = dict1['line_amt_sum']
                    # 写入数据库
                    #cursor.execute(
                    #    "insert into dbread.dim_custm_gd(date_id, city_name,region_com_name,custm_code,custm_name,custm_lever,demand_qty_t,demand_qty_x,sales_qty_t,sales_qty_x,sales_amt) values (?,?,?,?,?,?,?,?,?,?,?)",
                    #    (b1, b2, b3, b4, b5, b6, b8, b9, b10, b11, b12))

                #print('写入', city, j, '行数据')

            # 将修改动作进行提交
            #connStr.commit()
            #print(date_yest_sql,'的数据修改成功')
            # 查询
            # cursor.execute('''select case when pd='0' then '无重复数据' else pd end pd from ('''
            # select distinct case when count>=2 then DATE_ID||'  '||CITY_NAME||'数据重复' else '0' end pd
            # from (select date_id,
            #              city_name,
            #              custm_code,
            #              count(DATE_ID) count
            #       from DBREAD.DIM_CUSTM_GD
            #       group by date_id, city_name, custm_code)))
            # row = cursor.fetchone()
            # 将查询结果进行打印
            # while row:
            #     (col1) = (row[0])
            #     row = cursor.fetchone()
            #     print(col1)
            # # 关闭游标
            # cursor.close()
            # 退出链接
            # connStr.close()

#以上是爬档位数据，以下是爬投放周期数据

        url_cycle = 'https://data.gd.tobacco.com.cn:8443/query/dashboard/getAggregateData.do'
        headers_cycle = {
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': 'data.gd.tobacco.com.cn:8443',
            'Origin': 'https://data.gd.tobacco.com.cn:8443',
            'Referer': 'https://data.gd.tobacco.com.cn:8443/query/starter.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
        }
        post_form = {
            'datasetId': 137,
            'cfg': '{"rows":[{"columnName":"COM_TEXT","filterType":"=","values":[],"id":"bbb99a88-941d-4c16-bc7b-c9e0d212f51f"},{"columnName":"ITEM_NAME","filterType":"=","values":[],"id":"38d3a679-f3da-4398-b55e-60dc5b5558aa"},{"columnName":"WEEK","filterType":"eq","values":[],"id":"79896a98-c3c3-4684-babd-64b6116da2d7"},{"columnName":"BEGIN_DATE","filterType":"eq","values":[],"id":"9415cf23-2ca9-465b-9be9-7a33a0f74d83"},{"columnName":"END_DATE","filterType":"eq","values":[],"id":"48a9aadc-7cfa-4650-9f3e-8aa8aace6c91"}],"columns":[],"filters":[],"values":[{"column":"LMT_QTY","aggType":"sum"},{"column":"COMP_NUMBER","aggType":"sum"}]}',
            'reload': 'false'
        }

        s_cycle = requests.session()
        r_cycle = s_cycle.post(url=url_cycle, data=post_form, headers=headers_cycle,verify=False,cert="mgy.pem")
        print('爬周期登录状态码：',r_cycle)
        # 保存cookies
        c_cycle = requests.cookies.RequestsCookieJar()
        c_cycle.set("JSESSIONID", '083AB5363C967106A7F7091BCB5E5A42')
        s_cycle.cookies.update(c_cycle)

        respones_cycle = requests.post(url=url_cycle, data=post_form, headers=headers_cycle, cert="mgy.pem")
        print('查询状态码:',respones_cycle)
        list0_cycle = respones_cycle.json()['data']

        dsn_cycle = "driver={IBM DB2 ODBC DRIVER};database=%s;hostname=%s;port=%s;protocol=tcpip;uid=gdzyuser;pwd=f16Ysj9B@gdzyuser;" % (
        "GDZY2", "10.160.19.25", "50000")
        connStr_cycle = pyodbc.connect(dsn_cycle)
        cursor_cycle = connStr_cycle.cursor()
        print('链接数据库成功')
        j_cycle = 0
        for list1_cycle in list0_cycle:
            j_cycle = j_cycle + 1
            c1 = list1_cycle[0]
            c2 = list1_cycle[2]
            c3 = list1_cycle[3]
            c4 = list1_cycle[4]
            # 写入数据库
            cursor_cycle.execute(
                "insert into dbread.dim_date_cycle(city_name,sales_week,date_start,date_end) values (?,?,?,?)",
                (c1, c2, c3, c4))
        print('本次导入投放周期数据', j_cycle, '行')

        cursor_cycle.execute('''delete
        from DBREAD.DIM_DATE_CYCLE
        where id in
              (select ID
               from (select ID,
                            CITY_NAME,
                            SALES_WEEK,
                            DATE_START,
                            DATE_END,
                            row_number()
                                    over (partition by CITY_NAME,SALES_WEEK,DATE_START,DATE_END order by CITY_NAME,SALES_WEEK,DATE_START,DATE_END) rank
                     from DBREAD.DIM_DATE_CYCLE)
               where rank <> 1)''')
        # 将修改动作进行提交
        connStr_cycle.commit()

        cursor_cycle.execute('''select count(id) from DBREAD.DIM_DATE_CYCLE''')
        row_cycle = cursor_cycle.fetchone()[0]
        print('已删除投放周期表重复数据,共保留', row_cycle, '行数据')
        # 查询#关闭游标
        cursor_cycle.close()
        # 退出链接
        connStr_cycle.close()

        time_end = time.time()
        print('本次运行结束，运行时间：', (time_end - time_start) / 60,'分钟，等待下一次定时运行....')

        time.sleep(2)  # 因为以秒定时，所以暂停2秒，使之不会在1秒内执行多次