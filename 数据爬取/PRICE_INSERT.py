#  -*- coding: utf-8 -*-
# Project: 长沙价格导入
# Author:  yaochunling
# Created on 2020-02-22
#########################################################

import pandas as pd
import datetime
import ibm_db;

Start_time = datetime.datetime.now()

#读取原数据
csvfile = "/Users/yaochunling/Desktop/价格处理CSV 删空格.csv"   #文件存放路径
data_org = data = pd.read_csv(csvfile,header=0, encoding="GB18030")
print("一共需要处理的数据为："+ str(len(data_org)))

#涉及的城市
sample_codes = data_org[['SAMPLE_CODE']]
sample_codes.drop_duplicates(subset=['SAMPLE_CODE'],inplace=True)
cities = list(sample_codes.loc[:,'SAMPLE_CODE'].values)
print("数据插入的城市为：" + str(cities))
areas="("
for each_city in cities:
    areas = areas+"'"+each_city+"',"
areas = (areas+")").replace(',)',')')
#print (areas)

#涉及的月份
month_codes = data_org[['MONTH_ID']]
month_codes.drop_duplicates(subset=['MONTH_ID'],inplace=True)
MONTH_IDs = list(month_codes.loc[:,'MONTH_ID'].values)
print("数据插入的月份为：" + str(MONTH_IDs))
months="("
for each_month in MONTH_IDs:
    months = months+"'"+str(each_month)+"',"
months = (months+")").replace(',)',')')
#print (months)

#数据库已经存在的数据
conn  = ibm_db.connect("DATABASE=GDZY;HOSTNAME=10.160.19.25;PORT=50000;PROTOCOL=TCPIP;UID=gdzyuser;PWD=f16Ysj9B@gdzyuser;AUTHENTICATION=SERVER;", "", "")

data_existed = []
#20200221的日销量、月销量、年销量、当日库存
s_sql    = "SELECT BAR_CODE, MONTH_ID, DATE_ID, SAMPLE_CODE, CIRCL_PRICE FROM DBREAD.GD_XXK_CIRCL_PRICE "\
           " where MONTH_ID in " + str(months) +" and SAMPLE_CODE in " + str(areas)
print (s_sql)
stmt     = ibm_db.exec_immediate( conn, s_sql)
result   = ibm_db.fetch_both(stmt)    #一行一行的执行
while (result):
    data_existed.append(result)
    result = ibm_db.fetch_both(stmt)

data_existed = pd.DataFrame(list(data_existed),columns=['BAR_CODE', 'MONTH_ID', 'DATE_ID', 'SAMPLE_CODE', 'CIRCL_PRICE'])

PRE_SQL = "insert into DBREAD.GD_XXK_CIRCL_PRICE(BAR_CODE, BAR_NAME, MONTH_ID, DATE_ID, SAMPLE_CODE, PRICE_PLACE_NAME, CIRCL_PRICE) values"
i_sql = ""
count = 0  #记录新增的数据条数
for row in data_org.index:

    insert_data      = data_org.loc[row].tolist()
    bar_code         = str(insert_data[0])
    #print (insert_data[1])
    BAR_NAME         = insert_data[1]
    MONTH_ID         = str(insert_data[2])
    DATE_ID          = str(insert_data[3])
    SAMPLE_CODE      = str(insert_data[4])
    PRICE_PLACE_NAME = insert_data[5]
    CIRCL_PRICE      = insert_data[6]
    if (CIRCL_PRICE-int(CIRCL_PRICE))==0:
        CIRCL_PRICE = int(CIRCL_PRICE)

    #检查这句话是否已经存在：
    if len(data_existed[(data_existed['BAR_CODE']==bar_code) & (data_existed['DATE_ID']==DATE_ID) & (data_existed['SAMPLE_CODE']==SAMPLE_CODE)])>0:
        pass
    else:
        count = count+1
        i_sql = i_sql + " ('" + str(bar_code) + "', '" + BAR_NAME + "', '" + str(MONTH_ID) + "', '" + str(DATE_ID) + "', '" + str(SAMPLE_CODE) + "', '" + PRICE_PLACE_NAME + "', '" + str(CIRCL_PRICE) +"' ),"

print("执行数据插入数据库处理：")
if i_sql != "":
    i_sql = i_sql.rstrip(',')
    i_sql = PRE_SQL + i_sql
    print("i_sql:")
    print(i_sql)
    ibm_db.exec_immediate(conn, i_sql)
    ibm_db.commit(conn)
print("数据插入处理已经完成，一共插入数据条数为："+ str(count))

ibm_db.close(conn)

End_time = datetime.datetime.now()

print('程序执行时间为:'+str(End_time-Start_time))

print '程序结束....'