# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# Project: Brand Analysis
# Author:  yyg
# Created on 2017-10-12
##########################################
from com.ctitc.bigdata.entry.baseentry import BaseEntry
##########################################
# 数据库连接配置信息读取
##########################################
class DbEntry(BaseEntry):
    CONFIG_FILENM = "db.conf"
    ##########################################
    # 初始化配置文件
    ##########################################
    def __init__(self):
        super().__init__(self.CONFIG_FILENM)
        #read by type
        self.__db_host = self.cf.get("db","db_host")
        self.__db_port = int(self.cf.get("db","db_port"))
        self.__db_name = self.cf.get("db","db_name")
        self.__db_user = self.cf.get("db","db_user")
        self.__db_passwd = self.cf.get("db","db_passwd")

        self.__hv_host = self.cf.get("hive","db_host")
        self.__hv_port = int(self.cf.get("hive","db_port"))
        self.__hv_name = self.cf.get("hive","db_name")
        self.__hv_user = self.cf.get("hive","db_user")
        self.__hv_passwd = self.cf.get("hive","db_passwd")
    pass
    ##########################################
    # 数据库ip
    ##########################################
    @property
    def db_host(self):
        return self.__db_host
    pass
    ##########################################
    # 数据库port
    ##########################################
    @property
    def db_port(self):
        return self.__db_port
    pass
    ##########################################
    # 数据库名称
    ##########################################
    @property
    def db_name(self):
        return self.__db_name
    pass
    ##########################################
    # 数据库用户名
    ##########################################
    @property
    def db_user(self):
        return self.__db_user
    pass
    ##########################################
    # 数据库密码
    ##########################################
    @property
    def db_passwd(self):
        return self.__db_passwd
    pass

    ##########################################
    # 数据库ip
    ##########################################
    @property
    def hv_host(self):
        return self.__hv_host
    pass
    ##########################################
    # 数据库port
    ##########################################
    @property
    def hv_port(self):
        return self.__hv_port
    pass
    ##########################################
    # 数据库名称
    ##########################################
    @property
    def hv_name(self):
        return self.__hv_name
    pass
    ##########################################
    # 数据库用户名
    ##########################################
    @property
    def hv_user(self):
        return self.__hv_user
    pass
    ##########################################
    # 数据库密码
    ##########################################
    @property
    def hv_passwd(self):
        return self.__hv_passwd
    pass

pass
##########################################
if __name__ == "__main__":
    db = DbEntry()
    print("db_host=" + db.db_host)
    print("db_passwd=" + db.db_passwd)
pass