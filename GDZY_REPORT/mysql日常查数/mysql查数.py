from GDZY_REPORT.LOG.MyLog import logger
from GDZY_REPORT.Sql_param.sqlconfig import sql173
from GDZY_REPORT.Sql_param.sqlconfig import sql163
from GDZY_REPORT.SQL_LINK.MySql_link import MysqlDB

class MySqlConnect():
    def __init__(self,whichsql,sql):
        ##连接数据库
        self.sqlconfig=MysqlDB(whichsql)
        self.sqlconnect=self.sqlconfig.connect()
        self.data=self.sqlconfig.select(sql)
        logger.get_logger().info("运行的结果:\n%s",str(self.data))
        self.sqlconfig.close()
    pass
pass

###deep账号管理在173
#lyx123456
sql="select user_code,PASSWORD from deepcas.auth_user where user_name like('%罗文君%')"
##重置密码
sql_cz="update deepcas.auth_user set PASSWORD='000@A6xm' where user_code in('gdzy_lujc')"
# MySqlConnect(sql173,sql)
##有那些库
sql_ku="show databases"
#看有哪些表
sql_tab="show tables from deep"
#查看表结构
sql_tab_jg="desc deep.auth_department"
#查看创建数据表语句
sql_tab_jg="show create table deep.auth_department"
MySqlConnect(sql173,sql)


