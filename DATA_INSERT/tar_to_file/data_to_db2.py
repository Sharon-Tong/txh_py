# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/2 0:02 
'''

from DATA_INSERT.tar_to_file.gdzy_sj_tarfile import Unzip
from DATA_INSERT.tar_to_file.gdzy_sj_config import GdzyysParam
from GDZY_REPORT.LOG.MyLog import logger
from csv import reader
from GDZY_REPORT.SQL_LINK.DB2_link import MyDB2
import time


class InsertData():
    ##连接数据库
    def __init__(self):
        self.db      = MyDB2()
        # 连接数据库
        self.connect = self.db.connect()

    #批量读取excel表
    def get_all_xls(self):
        self.a=Unzip()
        self.zip_file   = self.a.get_file_name(GdzyysParam.zipifile,GdzyysParam.key_word)
        self.unzip_file = self.a.unkwzip(GdzyysParam.unzipfile)
        self.xlsfile    = Unzip().get_file_name(self.unzip_file)
        self.logger     = logger.get_logger()
        #print(self.xlsfile)

    def load_and_insert(self):
        self.logger.info('开始读csv...')
        for file in self.xlsfile:
            self.logger.info('现在开始处理:'+file)
            data  = reader(open(file))
            i=0
            s_sql = 'insert into ' + GdzyysParam.sql_tab + '('
            sql = ''
            for key, keyvalue in enumerate(GdzyysParam.sql_colname):
                sql = sql+keyvalue+','
                if key == len(GdzyysParam.sql_colname)-1:
                    sql = sql + keyvalue+') values'
            pass
            s_sql = s_sql+sql
            #print(s_sql)
            for row in data:
                i+=1
                insert_sql=''
                for key,keyvalue in enumerate(row):
                    insert_sql=insert_sql+"'"+str(keyvalue)+"',"
                    if key==len(row)-1:
                        insert_sql = "(" + insert_sql + str(keyvalue) + "')"
                    i_sql=s_sql+insert_sql
                pass
                # 操作数据库
                self.db.select(i_sql)
            self.logger.info('共处理数据%d条'%i)
            self.logger.info('%s已处理' % file)
        self.db.close()
        pass
    pass


if __name__ == '__main__' :
    start_time  = time.time()
    action      = InsertData()
    end_time    = time.time()
    m,s=divmod(end_time-start_time, 60)
    print("运行时间为：%02d分%02d秒" % (m,s))




# # A=reader(open(r'E:\GDZY_YSJG\test\test\I20440001_P99310101_C99310101_20200330_33_2e510171281e876113842020033033.csv'))
# # for i in A:
# #     print(i)
# a=InsertData()
# a.get_all_xls()
# a.load_xls()
#
# # print(a,b)















