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
import os

#########################################################
# 价值客户筛选评估分析模型
# RFM模型
#########################################################
class RFMImport(RFMBase):
    EXCEL_PATH = '/home/rasmode/tensorflow/data/hbzy'
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
        self.logger.info("============================RFM模型数据导入处理任务开始============================")
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
                tbl_1 = "" # 中烟零售户价值特征业务表
                tbl_2 = "" # 中烟核心零售户信息业务表
                tbl_3 = "" # 中烟核心零售户信息表
                if corp_code == "20420001":
                    # 20420001 湖北中烟
                    tbl_2 = "RFM_CORP_RTL_IMPORT_HBZY"
                else:
                    # 湖北中烟
                    tbl_2 = "RFM_CORP_RTL_IMPORT_HBZY"
                pass

                # 8. 各中烟核心客户模糊匹配:名称+地址模糊匹配
                file_name = "hbzy_" + self.busidate + ".xlsx"
                self.importData(file_name,tbl_2,'1')
                file_name = "hbzy_hn_" + self.busidate + ".xlsx"
                self.importData(file_name,tbl_2,'0')
                self.logger.info("==任务处理结束:%s", msg)
            except Exception as ex:
                self.logger.error(str(ex))
            pass
            cnt += 1
        pass
        self.logger.info(" 成功处理了 %s 任务.", cnt)
        self.logger.info("============================RFM数据导入处理任务结束============================")
    pass

    ##########################################
    # 主处理函数
    ##########################################
    def importData(self, file_name, tbl_name, is_del='0'):
        # 1.读入EXCEL文件
        # file_name = "hbzy_" + self.busidate + ".xlsx"
        # file_name = "hbzy_hn_201911.xlsx"
        file_path = os.path.join(self.EXCEL_PATH,file_name)
        data = pd.read_excel(file_path,header = 0)
        cols = "['" + "','".join(data.columns) + "']"
        data.columns = eval(cols.upper())
        self.logger.info(" data=%s",data.columns)
        self.logger.info(" data=%s",len(data))
        # 1.1数据清理
        # 换行符
        data = data.replace('\n','', regex=True)
        # '==>,
        data = data.replace({'R_NAME':'\''},'', regex=True)
        data = data.replace({'R_ADDRESS':'\''},' ', regex=True)
        data = data.replace({'R_XKZH':'\''},'', regex=True)
        data.fillna('', inplace=True)
        # 2.初始化数据库, 并配置表中查询任务数据
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()

            # 1 插入前先删除湖北中烟核心户业务表
            if is_del == '1':
                d_sql = "delete from " + tbl_name + " where BUSI_DATE = '" + self.busidate + "'"
                ret = db.delete(d_sql)
                self.logger.info(" RFM_CORP_RTL_IMPORT_HBZY删除成功= %s", str(ret))
            pass
            # 插入数据库
            PRE_SQL = "INSERT INTO " + tbl_name + "(BUSI_DATE, VISIT_CODE, VISIT_NAME, VISIT_TYPE, PROV_CODE, " +\
                      " PROV_NAME, CITY_CODE,CITY_NAME,DEPT_NAME, R_CODE,R_XKZH, R_NAME,R_ADDRESS," +\
                      " R_CONTACTOR,R_TEL, R_SLH, R_LABEL) VALUES "
            i_sql = ""
            count = 0
            values = {'R_XKZH': '', 'R_NAME': '', 'R_ADDRESS': '', 'VISIT_NAME': '', 'VISIT_TYPE': '',
                      'DEPT_NAME': '', 'R_SLH': '', 'R_LABEL': '', 'R_CONTACTOR': '', 'R_TEL': ''}
            data.fillna(value=values, inplace=True)
            data.fillna(0, inplace=True)
            for row in data.index:
                busi_date = str(data.loc[row]['BUSI_DATE'])
                visit_code = str(data.loc[row]['VISIT_CODE'])
                visit_name = data.loc[row]['VISIT_NAME']
                visit_type = str(data.loc[row]['VISIT_TYPE'])
                prov_code = str(data.loc[row]['PROV_CODE'])
                pro_name = data.loc[row]['PROV_NAME']
                city_code = str(data.loc[row]['CITY_CODE'])
                city_name = data.loc[row]['CITY_NAME']
                r_dept = data.loc[row]['DEPT_NAME']
                r_code = str(data.loc[row]['R_CODE'])
                r_xkzh = str(data.loc[row]['R_XKZH'])
                r_name = data.loc[row]['R_NAME']
                r_add = data.loc[row]['R_ADDRESS']
                r_contactor = data.loc[row]['R_CONTACTOR']
                r_tel = str(data.loc[row]['R_TEL'])
                r_slh = str(data.loc[row]['R_SLH'])
                r_label = data.loc[row]['R_LABEL']

                i_sql = i_sql + " ('" + busi_date + "', '" + visit_code + "', '" + visit_name + "', '" \
                        + visit_type + "', '" + prov_code + "', '" + pro_name + "','"  + city_code + "', '" \
                        + city_name + "', '" + r_dept + "', '" \
                        + r_code + "', '" + r_xkzh + "', '" + r_name + "','"  + r_add + "', '" \
                        + r_contactor + "', '" + r_tel + "','"  + r_slh + "','"  + r_label + "'),"
                count = count + 1
                if count%2000 == 0:
                    i_sql = i_sql.rstrip(',')
                    i_sql = PRE_SQL + i_sql
                    # self.logger.info(" i_sql = :%s\n",i_sql)
                    results = db.insert(i_sql)
                    i_sql = ""
                pass
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info(" 成功插入数据库记录数 = %s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql=%s",i_sql)
        finally:
            db.close()
        pass
    pass
pass
if __name__ == "__main__":
    # data = ['201910','201909','201908','201907','201906','201905','201904','201903','201902','201901']
    # data = ['201901','201902','201903','201904','201905','201906','201907','201908','201909','201910']
    # data = ['201910']
    # for row in data:
    #     rfm = RFMMatch()
    #     rfm.main(row)
    # pass
    model = RFMImport()
    model.main()
pass
