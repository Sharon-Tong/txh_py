# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/3 16:18 
'''

from csv import reader
from GDZY_REPORT.LOG.MyLog import logger
from GDZY_REPORT.Sql_param.sqlconfig import sql_myself

class csv_to_db2():
    #sql_type='db2' 是对DB2数据库进行操作，否则是对MYSQL进行操作
    #sql_base 是对应数据库类型的库
    def load_and_createsql(self,filename,tab,colnamelist):
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
        data = reader(open(filename))
        i = 0
        sql_value=[]
        #处理插入的内容
        for row in data:
            i += 1
            insert_sql = ''
            for key, keyvalue in enumerate(row):
                if key == len(row) - 1:
                    insert_sql = "(" + insert_sql + "'"+ str(keyvalue) + "')"
                else:
                    insert_sql = insert_sql + "'" + str(keyvalue) + "',"
                i_sql = s_sql + insert_sql
            pass
            sql_value.append(i_sql)
        pass
        self.logger.info('需要处理的数据共%d条' % i)
        return sql_value
        # #在数据库中插入数据
        # count=0
        # if sql_type == 'db2':
        #     db = MyDB2(sqlbase)
        # else:
        #     db = MysqlDB(sqlbase)
        #
        # for sql_insert in sql_value:
        #     #连接数据库
        #     db.connect()
        #     #增删查数据库
        #     a=db.select(sql_insert)
        #     if str(a)!='False':
        #         count += 1
        #     else:
        #         count =count
        #
        #
        # self.logger.info('共插入数据%d条' %count)
        # #关闭数据库
        #db.close()

# if __name__ == '__main__':
#     filename=r'D:\gdzy_work\数据插入\201904\test\I20440001_P11110000_C11110000_20200328_33_223301711dd1d6c1589d2020032833.csv'
#     sql_colname = ['x' + str(i) for i in range(25)]
#     a = csv_to_db2()
#     a.load_and_insert(filename=filename,tab='gdzy.test',colnamelist=sql_colname,sqlbase=sql_myself,sql_type='mysql')

