# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/2 8:36 
'''

class GdzyysParam():

    #压缩文件的路径
    zipifile    = r'E:\4.14\4.14gsp_zrz\1'

    #需要压缩的文件的关键字
    key_word    = 'DECRYPT'

    #压缩后的存放文件夹，路径仍然是zipfile
    unzipfile   ='unzip'

    #需要插入的数据库的名称
    sql_tab     ='gdzy.test'

    #变量名
    sql_colname = ['x'+str(i) for i in range(25)]

    #连接的数据库类型，如本地还是25库等
    db_base = 'MysqlDB'

    #连接的是mysql/db2
    db_type='mysql'





