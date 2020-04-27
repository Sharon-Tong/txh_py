# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# Project: Brand Analysis
# Author:  yyg
# Created on 2017-10-12
##########################################
from configparser import ConfigParser
import os
##########################################
# 配置信息读取父类
##########################################
class BaseEntry():
    ##########################################
    # 初始化配置文件
    ##########################################
    def __init__(self, fileNm=None):
        file_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = file_dir + "/" + fileNm
        self.path = full_path
        try:
            # 判断文件是否存在
            if(not os.path.exists(self.path)):
                raise Exception("file:" + self.path + " doesnot exist.")
            self.cf = ConfigParser()
            self.cf.read(self.path)
        except Exception as ex:
            print(" read db config file error. " + str(ex))
        pass
    pass
pass
##########################################
if __name__ == "__main__":
    db = BaseEntry()
pass