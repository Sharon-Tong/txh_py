#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2017-08-31
#########################################################
import numpy as np
from ctitc.db.mysqldb import MysqlDB
from ctitc.db.hivedb import HiveDB
from ctitc.model.rfm.rfm_base import RFMBase
import pandas as pd

from ctitc.model.rfm.rfm_rtl_kh_hbzy import RFMRtlKhHBZY
from ctitc.model.rfm.rfm_rtl_kh_syjt import RFMRtlKHSYJT
from ctitc.model.rfm.rfm_rtl_gzzy import RFMRtlGZZY

#########################################################
# 价值客户筛选评估分析模型
# RFM模型
#########################################################
class RFMRtl(RFMBase):
    ##########################################
    # 因子分析模型初始化
    ##########################################
    def __init__(self, busidate="201905"):
        super().__init__(logkey="rfm")
        self.busidate = busidate
    pass

    ##########################################
    # 主处理函数
    ##########################################
    def main(self, rq=''):
        # 0.初始化数据库, 并配置表中查询任务数据
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            s_total = "select CORP_CODE,CORP_NAME, GROUP_CODE, GROUP_NAME, BUSI_DATE, R_RATIO, F_RATIO,M_RATIO," \
                      " X_RATIO, ZJE_RATIO,ZDX_RATIO " \
                      " from RFM_CONFIG_INFO where IS_USE = '1' order by  CORP_CODE, GROUP_CODE "
            corpcodes = db.select(s_total)
        finally:
            db.close()
        pass
        if len(corpcodes) <= 0 :
            self.logger.error("record num is 0! exit system.")
            exit("record num is 0! exit system.")
        pass
        self.logger.info("============================RFM模型处理任务开始============================")
        # 1.遍历任务,进行处理
        cnt = 0
        ratio = {}
        for item in corpcodes:
            corp_code = item[0]
            corp_name = item[1]
            group_code = item[2]
            group_name = item[3]
            self.busidate = item[4]
            # self.busidate = rq
            ratio['r_ratio'] = item[5]
            ratio['f_ratio'] = item[6]
            ratio['m_ratio'] = item[7]
            ratio['x_ratio'] = item[8]
            ratio['zje_ratio'] = item[9]
            ratio['zdx_ratio'] = item[10]
            msg = corp_name + "(" + corp_code+ ") - "+ group_name + "("+group_code+")"
            self.logger.info("正在处理的任务:%s", msg)
            try:
                # 1. 确定中烟核心零售户信息表
                if corp_code == "20420001": # 湖北中烟
                    rfm = RFMRtlKhHBZY(busidate=self.busidate)
                    rfm.doProcess(corp_code,corp_name, group_code,group_name,ratio)
                elif corp_code == "11310001": # 上烟集团
                    rfm = RFMRtlKHSYJT(busidate=self.busidate)
                    rfm.doProcess(corp_code,corp_name, group_code,group_name,ratio)
                elif corp_code == "20520001": # 贵州中烟
                    rfm = RFMRtlGZZY(busidate=self.busidate)
                    rfm.doProcess(corp_code,corp_name, group_code,group_name,ratio)
                pass
                self.logger.info("==任务处理结束:%s", msg)
            except Exception as ex:
                self.logger.error(str(ex))
            pass
            cnt += 1
        pass
        self.logger.info(" 成功处理了 %s 任务.", cnt)
        self.logger.info("============================RFM模型处理任务结束============================")
    pass
pass
if __name__ == "__main__":
    # # data = ['201910','201909','201908','201907','201906','201905','201904','201903','201902','201901']
    # data = ['201901','201902','201903','201904','201905','201906','201907','201908','201909','201910']
    # data = ['201911','201910']
    # for row in data:
    #     rfm = RFMRtl()
    #     rfm.main(row)
    # pass
    rfm = RFMRtl()
    rfm.main()
pass
