# -*- coding: utf-8 -*-

import ibm_db
import json
from DATA_INSERT.LOG.MyLog import logger
#from GDZY_REPORT.Sql_param.sqlconfig import sql173
from DATA_INSERT.Sql_param.sqlconfig import db88
from DATA_INSERT.Sql_param.sqlconfig import db25



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
            # self.logger.info("连接数据库...")
            return self.conn
        except:
            self.logger.error("connect failed,%s" % ibm_db.conn_errormsg())
    pass

    #查询、增、删操作
    def select(self,sql):
        try:
            data_db2=[]
            self.stmt=ibm_db.exec_immediate(self.conn,sql)
            self.result=ibm_db.fetch_assoc(self.stmt)
            # self.logger.info("sql语句是："+sql)
            #存储为字典
            while self.result:
                data_db2.append(self.result)
                self.result=ibm_db.fetch_assoc(self.stmt)
            return data_db2
        except:
        # except Exception as ex:
            self.logger.error("stmt failed,%s" % ibm_db.stmt_errormsg())
            return False
    pass

    #查询、增、删操作
    def update(self,sql):
        try:
            self.stmt=ibm_db.exec_immediate(self.conn,sql)
            ibm_db.commit(self.conn)

        except:
        # except Exception as ex:
            self.logger.error("stmt failed,%s" % ibm_db.stmt_errormsg())
            return False

    pass
    #
    # def dict_to_list(self,data):
    #     data_r=[]
    #     for key,value in data:
    #         data_r[]


    def close(self):
        if (self.conn):
            ibm_db.close(self.conn)
            self.logger.info("关闭DB2数据库...")
    pass

#
# if __name__=="__main__":
#     a=MyDB2(db25)
#     a.connect()
#     data=a.select("select * from dbread.b_bar fetch first 1 rows only")
#     a.close()
#     print(data)





