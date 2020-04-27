import pymysql
from GDZY_REPORT.LOG.MyLog import logger
from GDZY_REPORT.Sql_param.sqlconfig import sql173
from GDZY_REPORT.Sql_param.sqlconfig import sql_myself

class MysqlDB():
    ##########################################
    # 初始化
    ##########################################
    def __init__(self,config):
        db = config
        self.host = db.db_host
        self.port = int(db.db_port)
        self.user = db.db_user
        self.passwd = db.db_passwd
        self.db = db.db_name
        self.logger=logger.get_logger()
    pass
    ##连接数据库
    def connect(self):
        try:
            self.conn = pymysql.Connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd,
                                        db=self.db,charset='utf8')

            self.cur = self.conn.cursor()
            self.logger.info('正在连接数据库...')
        except Exception as ex:
            self.logger.error(" connect error:%s",str(ex))
            # print("connect mysql error." + str(ex))
    pass
    # 查询记录,返回cursor
    ##########################################
    def select(self,sql):
        try:
            # print(" select sql =: %s " % sql)
            self.cur.execute(sql)
            self.conn.autocommit(True)
            #self.logger.info("sql语句:\n%s",sql)
        except Exception as ex:
            self.logger.error(" select error:\n%s",str(ex))
            self.logger.error(" select sql=:\n%s",sql)
            return False
            # print("sql = " + sql + "\n select error: " + str(ex))
        else:
            # print(" select row = %d"%self.cur.rowcount)
            return self.cur.fetchall()
    pass
    # 关闭数据库
    ##########################################
    def close(self):
        if(self.cur):
            self.cur.close()
            #self.logger.info("关闭数据库....")
        pass
        if(self.conn):
            self.conn.close()
            self.logger.info("关闭数据库....")
        pass
    pass

# if __name__ == '__main__':
#     a=MysqlDB(sql_myself)
#     a.connect()
#     data=a.select('show databases')
#     print(data)
#     a.close()

