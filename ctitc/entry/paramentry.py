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
class ParamEntry(BaseEntry):
    CONFIG_FILENM = "param.conf"
    ##########################################
    # 初始化配置文件
    ##########################################
    def __init__(self):
        super().__init__(self.CONFIG_FILENM)
        #read by type
        self.__log_path = self.cf.get("log","log_path")
    pass
    ##########################################
    # Log日志文件
    ##########################################
    @property
    def log_path(self):
        return self.__log_path
    pass
pass
##########################################
if __name__ == "__main__":
    param = ParamEntry()
    print("log_file=" + param.log_path)
pass