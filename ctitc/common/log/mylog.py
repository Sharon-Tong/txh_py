#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author:  yyg
# Created on 2018-02-25
#########################################################
import logging
import logging.config
from com.ctitc.bigdata.entry.paramentry import ParamEntry
import os
#########################################################
# 日志处理类
#########################################################
class MyLog():
    param = ParamEntry()
    log_path = param.log_path
    # LOG_PATH = "/home/rasmode/tensorflow/log/"
    # LOG_PATH = "e:/"
    ##########################################
    # 初始化
    ##########################################
    def __init__(self):
        # param = ParamEntry()
        # self.__log_file = param.log_file
        pass
    pass
    ##########################################
    # 获取Logger
    ##########################################
    @classmethod
    def getLogger(cls, name="root", log_file="logger.conf"):
        CONF_LOG = os.path.join(cls.log_path, log_file)
        print(CONF_LOG)
        logging.config.fileConfig(CONF_LOG)
        return logging.getLogger(name)
    pass
pass
if __name__ == '__main__':
    log = MyLog.getLogger("multi_ts","logger.conf")
    log.info("\nKMO检验值:\n%s", 123)
    log.error("error")
    # p = os.path.join("e:\c:\\", "exe.txt")
    # print(p)


