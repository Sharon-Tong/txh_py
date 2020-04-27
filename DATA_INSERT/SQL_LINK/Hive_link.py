# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/7 11:49 
'''

#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2017-08-31
###########################
from pyhive import hive
from DATA_INSERT.LOG.MyLog import logger
from DATA_INSERT.Sql_param.sqlconfig import gd_hive

##########################################
# pyhive数据库操作
class MyHive():
    ##########################################
    # 初始化
    ##########################################
    def __init__(self, dbconf):
        db = dbconf
        self.host = db.db_host
        self.port = db.db_port
        self.user = db.db_user
        self.passwd = db.db_passwd
        self.database = db.db_name

        self.logger = logger.get_logger()
    pass
    ##########################################
    # hive数据库连接
    ##########################################
    def connect(self):
        try:

            self.conn = hive.Connection(host=self.host, port=self.port, database=self.database,
                                        username=self.user, auth='LDAP',password=self.passwd)

            self.cur = self.conn.cursor()

        except Exception as ex:
            self.logger.error(" connect error:\n%s",str(ex))
    pass
    ##########################################
    # 查询记录,返回cursor
    ##########################################
    def select(self,sql):
        try:
            # print(" select sql =: %s " % sql)
            self.cur.execute(sql)
            self.conn.commit()
        except Exception as ex:
            self.logger.error(" select error:\n%s",str(ex))
            self.logger.error(" select sql=:\n%s",sql)
            # print("sql = " + sql + "\n select error: " + str(ex))
        else:
            # print(" select row = %d"%self.cur.rowcount)
            return self.cur.fetchall()
        pass
    pass


    ##########################################
    # 关闭数据库
    ##########################################
    def close(self):
        if(self.cur):
            self.cur.close()
            self.logger.info('hive连接关闭...')
        pass
        if(self.conn):
            self.conn.close()
        pass
    pass
pass


# ##########################################
# if __name__ == "__main__":
#     print("main")
#     db = MyHive(gd_hive)
#     db.connect()
#     db.select("select * from gdzy_dm_yx.DIM_FACTR limit 10")
# pass
# ################################
