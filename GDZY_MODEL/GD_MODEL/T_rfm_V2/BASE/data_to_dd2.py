# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/3 16:18 
'''

from csv import reader
from GDZY_MODEL.GD_MODEL.LOG.MyLog import logger
# from GDZY_MODEL.GD_MODEL.Sql_param.sqlconfig import sql_myself,
from GDZY_MODEL.GD_MODEL.SQL_LINK.DB2_link import MyDB2
from GDZY_MODEL.GD_MODEL.SQL_LINK.MySql_link import MysqlDB
import pandas as pd
class data_to_db():
    #sql_type='db2' 是对DB2数据库进行操作，否则是对MYSQL进行操作
    #sql_base 是对应数据库类型的库
    def load_and_createsql(self,data,tab,colnamelist):
        self.logger=logger.get_logger()
        #sql 插入的列名
        s_sql = 'insert into ' + tab + '('
        sql = ''
        for key, keyvalue in enumerate(colnamelist):
            if key == len(colnamelist) - 1:
                sql = sql + keyvalue + ') values'
            else:
                sql = sql + keyvalue + ','
        pass
        s_sql = s_sql + sql
        # data = reader(open(filename,encoding='utf-8'))
        i = 0
        sql_value=[]
        # print(colnamelist)
        #处理插入的内容
        for index,row in data.iterrows():
            i += 1
            insert_sql = ''
            for key, keyvalue in enumerate(colnamelist):

                if key == len(colnamelist) - 1:
                    insert_sql = "(" + insert_sql + "'"+ str(row[keyvalue]) + "'),"
                else:
                    insert_sql = insert_sql + "'" + str(row[keyvalue]) + "',"
                # i_sql = s_sql + insert_sql
                pass
            sql_value.append(insert_sql)
        pass
        self.logger.info('需要处理的数据共%d条' % i)
        return s_sql,sql_value


# if __name__ == '__main__':
#     filename=r'C:\Users\sharon_Tong\Desktop\test.csv'
#     data = reader(open(filename, encoding='utf-8'))
#
#     sql_colname = ['week_id','provc_code','city_code','bar_code','cust_code','order_amt','order_qty']
#     data = pd.DataFrame(data, columns=sql_colname)
#     a = data_to_db()
#     sql_value=a.load_and_createsql(data,tab='gdzy.test',colnamelist=sql_colname)
# #     # print(sql_value)
# #     for i in sql_value:
# #         print(i)
#     b=MysqlDB(sql_myself)
#     b.connect()
#     for i in sql_value:
#         b.select(i)
#     b.close()



