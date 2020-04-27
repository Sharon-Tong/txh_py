# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/3 16:58 
'''
from GDZY_REPORT.Sql_param.sqlconfig import sql_myself
from DATA_INSERT.tar_to_file.gdzy_sj_tarfile import Unzip
from DATA_INSERT.tar_to_file.gdzy_sj_config import GdzyysParam
from GDZY_REPORT.LOG.MyLog import logger
from GDZY_REPORT.SQL_LINK.DB2_link import MyDB2
from GDZY_REPORT.SQL_LINK.MySql_link import MysqlDB
import time
from DATA_INSERT.tar_to_file.csv_to_dd2 import csv_to_db2
class InsertData():
    #批量读取excel表
    def __init__(self,db_base):
        self.para=GdzyysParam
        self.logger=logger.get_logger(log_path=r'/DATA_INSERT/LOG.txt')
        if self.para.db_type == 'db2':
            self.db = MyDB2(db_base)
        else:
            self.db = MysqlDB(db_base)
        # 连接数据库
        self.db.connect()


    def get_all_xls(self):
        self.a          = Unzip()
        self.zip_file   = self.a.get_file_name(GdzyysParam.zipifile,GdzyysParam.key_word)
        self.unzip_file = self.a.unkwzip(GdzyysParam.unzipfile,zip_type='zip')
        self.xlsfile    = Unzip().get_file_name(self.unzip_file)
        self.logger     = logger.get_logger()
        #print(self.xlsfile)

    def load_and_insert(self):
        self.logger.info('开始读csv...')
        all_count=0
        i=0
        for file in self.xlsfile:
            count = 0
            i+=1
            self.logger.info('开始处理第%d个csv文件:%s:' % (i,file))
            # 读取文件,并返回sql语句
            sql_value=csv_to_db2().load_and_createsql(file,self.para.sql_tab,self.para.sql_colname)

            # 在数据库中插入数据
            # for sql_insert in sql_value:
            #
            #     # 增删查数据库
            #     a = self.db.select(sql_insert)
            #     if str(a) != 'False':
            #         count += 1
            #     else:
            #         count = count

            self.logger.info('共插入数据%d条' % count)

            all_count+=count
        self.logger.info('共有%d个文件，总共插入数据%d条' % (len(self.xlsfile),all_count))
        # 关闭数据库
        self.db.close()

        pass

if __name__ == '__main__':
    start_time = time.time()
    a=InsertData(sql_myself)
    a.get_all_xls()
    a.load_and_insert()
    end_time    = time.time()
    m,s=divmod(end_time-start_time, 60)
    print("运行时间为：%02d分%02d秒" % (m,s))