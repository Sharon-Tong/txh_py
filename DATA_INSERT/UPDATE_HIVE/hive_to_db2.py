# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/16 18:50 
'''

from DATA_INSERT.SQL_LINK.Hive_link import MyHive
from DATA_INSERT.SQL_LINK.DB2_link import MyDB2
from DATA_INSERT.Sql_param.sqlconfig import db25,gd_hive,db25_1
from DATA_INSERT.UPDATE_HIVE.create_sql import data_to_db
from DATA_INSERT.LOG.MyLog import logger
from DATA_INSERT.UPDATE_HIVE.config import DBParam

#从hive中获取数据
class get_data_to_db():

    def __init__(self):
        self.hive_object = MyHive(gd_hive)
        # self.hive_object = MyDB2(db25)
        self.db_object   = MyDB2(db25)

        #连接数据库
        self.hive_object.connect()
        self.db_object.connect()

    def data_from_hive_to_db2(self,sql,tab_name,col_list):
        # sql = 'select * from gdzy_dm_yx.DIM_CIGRT_ALL'

        data = self.hive_object.select(sql)

        # data.replace('None','null')

        logger.get_logger().info('开始处理表：' + tab_name + '...')
        logger.get_logger().info('现在delete表:' + tab_name + '...')

        self.db_object.update('delete from '+ tab_name)

        logger.get_logger().info('准备更新表' + tab_name + '...')

        s_sql,sql_tab = data_to_db().load_and_createsql(data,tab_name,col_list)

        # print(s_sql+sql_tab[1])
        count = 0
        sql = ''
        for sql_tab_i in sql_tab:

            sql = sql + sql_tab_i
            # print(sql)
            # if count>=10 and count<=11:
            #     print(s_sql + sql.rsplit(',', 1)[0])
            # if count<=1045:
            #     print(s_sql + sql.rsplit(',', 1)[0])
            #     break

            count = count + 1
            if count % 1000 == 0:
                i_sql = s_sql + sql.rsplit(',', 1)[0]
                sql = ''
                self.db_object.update(i_sql)
                # print(i_sql)
                print('正在插入第%d条数据' % count)

        i_sql = s_sql + sql.rsplit(',', 1)[0]
        self.db_object.update(i_sql)
        print('全部数据已插入，共插入数据%d条' % count)

    def con_close(self):
        self.hive_object.close()
        self.db_object.close()



if __name__ == "__main__" :

    ab= get_data_to_db()
    ab.data_from_hive_to_db2('select * from gdzy_dm_yx.DIM_CIGRT_ALL',DBParam.tab_name_cigrt,DBParam.col_list_cigrt)
    ab.data_from_hive_to_db2('select * from gdzy_dm_yx.dim_city', DBParam.tab_name_city, DBParam.col_list_city)
    ab.data_from_hive_to_db2('select * from gdzy_dm_yx.dim_date', DBParam.tab_name_date, DBParam.col_list_date)
    ab.con_close()


    # sql ='''
    # SELECT DISTINCT PROVC_CODE,PROVC_NAME,CITY_CODE,CITY_NAME,'0' AS flag,
    # ROW_NUMBER()over(ORDER BY PROVC_CODE)  AS index_id FROM DBREAD.DIM_CITY dc WHERE PROVC_CODE<>'湖北省'
    # '''
    # ab.data_from_hive_to_db2(sql,DBParam.tem_tab, DBParam.col_list_tem)





