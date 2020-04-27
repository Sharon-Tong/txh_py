#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2017-08-31
###########################
import pymysql

from com.ctitc.bigdata.entry.dbentry import DbEntry
from com.ctitc.bigdata.common.log.mylog import MyLog
##########################################
# mysql数据库操作
class MysqlDB():
    ##########################################
    # 初始化
    ##########################################
    def __init__(self, dbconf=None, logger=None):
        db = (dbconf if dbconf is not None else DbEntry())
        self.host = db.db_host
        self.port = int(db.db_port)
        self.user = db.db_user
        self.passwd = db.db_passwd
        self.db = db.db_name
        if logger is None:
            logger = MyLog.getLogger("default", log_file='logger.conf')
        pass
        self.logger = logger
    pass
    ##########################################
    # 数据库连接
    ##########################################
    def connect(self):
        try:
            self.conn = pymysql.Connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd,
                                        db=self.db,charset='utf8')
            self.conn.autocommit(False)
            self.cur = self.conn.cursor()
        except Exception as ex:
            self.logger.error(" connect error:\n%s",str(ex))
            # print("connect mysql error." + str(ex))
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
        count = 0
        try:
            # print(" delete sql =: %s " % sql)
            self.cur.execute(sql)
        except(Exception) as ex:
            # print("sql = " + sql + "\n delete error: " + str(ex))
            self.conn.rollback()
            self.logger.error(" delete error:\n%s",str(ex))
            self.logger.error(" delete sql=:\n%s",sql)
        else:
            count = self.cur.rowcount
            self.conn.commit()
        pass
        return count
            # print(" delete row = %d"%self.cur.rowcount)
    pass
    ##########################################
    # 更新记录
    ##########################################
    def update(self,sql):
        count = 0
        try:
            self.cur.execute(sql)
        except(Exception) as ex:
            # print("sql = " + sql + "\n update error: " + str(ex))
            self.conn.rollback()
            self.logger.error(" update error:\n%s",str(ex))
            self.logger.error(" update sql=:\n%s",sql)
        else:
            count = self.cur.rowcount
            self.conn.commit()
            # print(" update row = %d"%self.cur.rowcount)
        pass
        return count
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
    # db = MysqlDB()
    # db.connect()
    # db.select("select * from master_user_info")

    tmp = "sadfas'f'"
    tmp = tmp.replace("'","")
    print(tmp)
    ret = pymysql.escape_string("sad'fasf'")
    print(ret)
    ret.replace("\'","")
    print(ret)

pass
################################
