#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2017-08-31
###########################
from pyhive import hive

from ctitc.entry.dbentry import DbEntry
from ctitc.common.log.mylog import MyLog
##########################################
# pyhive数据库操作
class HiveDB():
    ##########################################
    # 初始化
    ##########################################
    def __init__(self, dbconf=None, logger=None):
        db = (dbconf if dbconf is not None else DbEntry())
        self.host = db.hv_host
        self.port = int(db.hv_port)
        self.user = db.hv_user
        self.passwd = db.hv_passwd
        self.database = db.hv_name
        if logger is None:
            logger = MyLog.getLogger("default", log_file='logger.conf')
        pass
        self.logger = logger
    pass
    ##########################################
    # hive数据库连接
    ##########################################
    def connect(self):
        try:
            # constr=self.host + " " + self.port + self.database + self.user + self.passwd
            # print(self.host + " " + self.)
            self.conn = hive.Connection(host=self.host, port=self.port, database=self.database,
                                        kerberos_service_name=self.user, auth=self.passwd)
            # self.conn = hive.Connection(host='hadoop02.ctitc.com', port=10000, database='usertmp',
            #                             auth='KERBEROS',kerberos_service_name='hive')
            self.cur = self.conn.cursor()
            # self.cur.execute("SELECT BUSI_DATE, PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME,  R_KHTYBM, R_KHBM, R_XKZH, R_NAME,R_ADDRESS FROM  usertmp.RFM_RTL_FEATURE_HBZY_201908 WHERE  BUSI_DATE='201907' limit 100")
            # print(self.cur.rowcount)
            # for result in self.cur.fetchall():
            #     print(result)
        except Exception as ex:
            self.logger.error(" connect error:\n%s",str(ex))
            # print("connect hive error." + str(ex))
    pass
    ##########################################
    # 查询记录,返回cursor
    ##########################################
    def select(self,sql):
        try:
            # print(" select sql =: %s " % sql)
            self.cur.execute(sql)
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
    # 插入记录
    ##########################################
    def insert(self,sql):
        count = 0
        try:
            # print(" insert sql =: %s " % sql)
            self.cur.execute(sql)
        except(Exception) as ex:
            # print("sql = " + sql + "\n insert error: " + str(ex))
            self.conn.rollback()
            self.logger.error(" insert error:\n%s",str(ex))
            self.logger.error(" insert sql=:\n%s",sql)
        else:
            count = self.cur.rowcount
            self.conn.commit()
            # print(" insert row = %d"%self.cur.rowcount)
        pass
        return count
    pass
    ##########################################
    # 删除记录
    ##########################################
    def delete(self,sql):
        try:
            # print(" delete sql =: %s " % sql)
            self.cur.execute(sql)
        except(Exception) as ex:
            # print("sql = " + sql + "\n delete error: " + str(ex))
            self.conn.rollback()
            self.logger.error(" delete error:\n%s",str(ex))
            self.logger.error(" delete sql=:\n%s",sql)
        else:
            self.conn.commit()
            # print(" delete row = %d"%self.cur.rowcount)
    pass
    ##########################################
    # 更新记录
    ##########################################
    def update(self,sql):
        try:
            self.cur.execute(sql)
        except(Exception) as ex:
            # print("sql = " + sql + "\n update error: " + str(ex))
            self.conn.rollback()
            self.logger.error(" update error:\n%s",str(ex))
            self.logger.error(" update sql=:\n%s",sql)
        else:
            self.conn.commit()
            # print(" update row = %d"%self.cur.rowcount)
    pass

    ##########################################
    # 关闭数据库
    ##########################################
    def close(self):
        if(self.cur):
            self.cur.close()
        pass
        if(self.conn):
            self.conn.close()
        pass
    pass
pass
##########################################
if __name__ == "__main__":
    print("main")
    db = HiveDB()
    db.connect()
    db.select("select * from master_user_info")
pass
################################
