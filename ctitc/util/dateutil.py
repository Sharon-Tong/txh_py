#!/usr/bin/env python
#  -*- coding: utf-8 -*-
# Project: Brand Analysis
# Author:  yyg
# Created on 2017-10-12
##########################################
import time
import numpy as np
import datetime
# from dateutil.relativedelta import relativedelta

##########################################
#
# 日期辅助类
#
##########################################
class DateUtil():

    ##########################################
    # 生成14位序列号
    ##########################################
    @staticmethod
    def get_seq():
        rtl = time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))

        return rtl
    pass
    ##########################################
    # 生成14位序列号
    ##########################################
    @staticmethod
    def get_nowtime():
        rtl = time.strftime('%Y%m%d %H:%M:%S',time.localtime(time.time()))

        return rtl
    pass
    ##########################################
    # 生成14位序列号
    ##########################################
    @staticmethod
    def get_nowym():
        rtl = time.strftime('%Y%m',time.localtime(time.time()))
        return rtl
    pass
    ##########################################
    # 生成14位序列号
    ##########################################
    @staticmethod
    def get_taskcode(pre_code=''):
        rtl = time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))
        rtl = pre_code + "_" + rtl
        return rtl
    pass

pass
##########################################
if __name__ == "__main__":
    strs = '201812'
    # date = datetime.datetime.strptime(strs,'%Y%m')
    # rtl = time.strftime('%Y%m',time.localtime(time.time()))
    #
    # print(rtl)
    # delta = date + relativedelta(months=1)
    # rtn = delta.strftime('%Y%m')
    # print(rtn)

pass
################################
