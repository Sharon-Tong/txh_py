# -*- coding: utf-8 -*-

import ibm_db
import json
from GDZY_REPORT.LOG.MyLog import logger
#from GDZY_REPORT.Sql_param.sqlconfig import sql173
from GDZY_REPORT.Sql_param.sqlconfig import db88
from GDZY_REPORT.Sql_param.sqlconfig import db25
import demjson

class MyDB2():
    ##########################################
    # 初始化
    ##########################################
    def __init__(self,config):
        db = config
        self.host = db.db_host
        self.port = db.db_port
        self.user = db.db_user
        self.passwd = db.db_passwd
        self.db = db.db_name
        self.conn=None
        self.logger=logger.get_logger()

    pass
    #连接数据库
    def connect(self):
        try:
            self.conn = ibm_db.connect("DATABASE="+ self.db +";HOSTNAME="+self.host+";PORT="+self.port + \
                                  ";PROTOCOL=TCPIP;UID="+self.user + ";PWD=" + self.passwd + \
                                  ";AUTHENTICATION=SERVER;", "", "")
            self.logger.info("连接数据库...")
        except:
            self.logger.error("connect failed,%s" % ibm_db.stmt_errormsg())
    pass

    #查询、增、删操作
    def select(self,sql):
        try:
            data_db2=[]
            self.stmt=ibm_db.exec_immediate(self.conn,sql)
            ibm_db.commit(self.conn)
            #self.result=ibm_db.fetch_both(self.stmt)
            self.result=ibm_db.fetch_assoc(self.stmt)
            #self.logger.info("sql语句是："+sql)
            #print(self.result)
            ##存储为字典
            while self.result:
                data_db2.append(self.result)
                self.result=ibm_db.fetch_assoc(self.stmt)
        except Exception as ex:
            self.logger.error(" select error:\n%s", str(ex))
            #self.logger.error(" select sql=:\n%s", sql)
        return data_db2
    pass
    #
    # def dict_to_list(self,data):
    #     data_r=[]
    #     for key,value in data:
    #         data_r[]


    def close(self):
        if (self.conn):
            ibm_db.close(self.conn)
            self.logger.info("关闭数据库...")
    pass


if __name__=="__main__":
    a=MyDB2(db25)
    a.connect()
    data=a.select("select * from dbread.b_bar fetch first 10 rows only")
    a.close()
    print(data)





