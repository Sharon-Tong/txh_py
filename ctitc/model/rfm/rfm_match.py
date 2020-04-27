#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2017-08-31
#########################################################
import numpy as np
from com.ctitc.bigdata.db.mysqldb import MysqlDB
from com.ctitc.bigdata.db.hivedb import HiveDB
from com.ctitc.bigdata.model.rfm.rfm_base import RFMBase
import pandas as pd

#########################################################
# 价值客户筛选评估分析模型
# RFM模型
#########################################################
class RFMMatch(RFMBase):
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
        self.logger.info("============================RFM模型模糊匹配处理任务开始============================")
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
                    tbl_1 = "usertmp.RFM_RTL_FEATURE_HBZY"
                    # tbl_1 = "RFM_RTL_FEATURE_HBZY_ONE_MONTH"
                    tbl_2 = "RFM_CORP_RTL_HBZY"
                    tbl_3 = "RFM_RTL_HBZY"

                else:
                    # 湖北中烟
                    tbl_1 = "usertmp.RFM_RTL_FEATURE_HBZY"
                    # tbl_1 = "RFM_RTL_FEATURE_HBZY_ONE_MONTH"
                    tbl_2 = "RFM_CORP_RTL_HBZY"
                    tbl_3 = "RFM_RTL_HBZY"
                pass

                # 8. 各中烟核心客户模糊匹配:名称+地址模糊匹配
                self.fuzzyMatchRtl(corp_code, corp_name, group_code,tbl_2, tbl_3)
                self.logger.info("==任务处理结束:%s", msg)
            except Exception as ex:
                self.logger.error(str(ex))
            pass
            cnt += 1
        pass
        self.logger.info(" 成功处理了 %s 任务.", cnt)
        self.logger.info("============================RFM模型模糊匹配处理任务结束============================")
    pass
    ##########################################
    # 各中烟核心客户模糊匹配:名称+地址模糊匹配
    ##########################################
    def fuzzyMatchRtl(self, corp_code, corp_name, group_code,tbl_2, tbl_3):
        ratio = 0.8
        db = MysqlDB(logger=self.logger)
        i_sql = ""
        zy_err_df = None
        try:
            db.connect()
            # 1.0 获取tbl_2:RFM_CORP_RTL_HBZY的拜访零售户
            s_sql = "SELECT BUSI_DATE, PROV_CODE,PROV_NAME, CITY_CODE, CITY_NAME, DEPT_NAME, R_CODE, " + \
                    " R_NAME, R_ADDRESS, R_XKZH, R_CONTACTOR, R_TEL, R_SLH, R_LABEL  " + \
                    " FROM  " + tbl_2  + \
                    " order by PROV_CODE, CITY_CODE"
            zy_bfh_rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            zy_bfh_df = pd.DataFrame(list(zy_bfh_rec), columns=['BUSI_DATE','PROV_CODE','PROV_NAME','CITY_CODE','CITY_NAME',
                                                                'DEPT_NAME','R_CODE','R_NAME','R_ADDRESS', 'R_XKZH',
                                                                'R_CONTACTOR','R_TEL','R_SLH','R_LABEL'])
            zy_bfh_df.drop_duplicates(subset=['PROV_CODE', 'CITY_CODE', 'R_CODE'],inplace=True)
            self.logger.info(" zy_bfh_df=%s",len(zy_bfh_df))
            self.logger.info(" zy_bfh_df=%s",zy_bfh_df.columns)

            # 2.0 获取tbl_3:RFM_RTL_HBZY的核心零售户
            # s_sql = "SELECT PROV_CODE, CITY_CODE, R_CODE, R_XKZH, R_NAME, R_ADDRESS " + \
            #         " FROM  " + tbl_3 + "   order by PROV_CODE, CITY_CODE"
            # zy_hxh_rec = db.select(s_sql)
            # self.logger.info(" SQL=:\n%s",s_sql)
            # zy_hxh_df = pd.DataFrame(list(zy_hxh_rec), columns=['PROV_CODE', 'CITY_CODE', 'R_CODE',
            #                                                     'R_XKZH','R_NAME', 'R_ADDRESS'])
            # valid_df = pd.merge(zy_bfh_df, zy_hxh_df.drop(['R_XKZH', 'R_NAME', 'R_ADDRESS'], axis=1, inplace=False),
            #                     how='inner', on=['PROV_CODE','CITY_CODE','R_CODE'])
            # self.logger.info(" valid_df=%s",len(valid_df))
            # self.logger.info(" valid_df=%s",valid_df.columns)
            # zy_bfh_df.append(valid_df)
            # zy_bfh_df = zy_bfh_df.drop_duplicates(subset=['PROV_CODE', 'CITY_CODE', 'R_CODE'], keep=False,inplace=True)
            # self.logger.info(" zy_bfh_df=%s",len(zy_bfh_df))
            # self.logger.info(" zy_bfh_df=%s",zy_bfh_df.columns)

            # 3.0 获取tbl_3:RFM_CORP_RTL_ERR的核心零售户
            s_sql = "SELECT PROV_CODE, CITY_CODE, R_CODE " + \
                    " FROM  RFM_CORP_RTL_ERR  " + \
                    " WHERE  CORP_CODE='" + corp_code + "' " + \
                    " and GROUP_CODE='" + group_code + "' order by PROV_CODE, CITY_CODE"
            zy_err_rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            zy_err_df = pd.DataFrame(list(zy_err_rec), columns=['PROV_CODE', 'CITY_CODE', 'R_CODE'])
            zy_err_df.drop_duplicates(subset=['PROV_CODE', 'CITY_CODE', 'R_CODE'],inplace=True)
            zy_err_df = pd.merge(zy_bfh_df, zy_err_df,  how='inner', on=['PROV_CODE','CITY_CODE','R_CODE'])
            self.logger.info(" zy_err_df=%s",len(zy_err_df))
            self.logger.info(" zy_err_df=%s",zy_err_df.columns)

        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql=%s",i_sql)
        finally:
            db.close()
        pass

        codes_df = zy_err_df[['PROV_CODE','CITY_CODE']]
        city_codes = codes_df.drop_duplicates(subset=['PROV_CODE','CITY_CODE'], inplace=False)
        # 2.0 从大数据平台获取信息
        hdb = HiveDB(logger=self.logger)
        all_df = None
        arr_code = []
        try:
            hdb.connect()
            for crow in city_codes.index:
                city_code = city_codes.loc[crow,'CITY_CODE']
                s_sql = "SELECT RETAILER_CODE, R_NAME, R_ADDRESS FROM SRC.DIM_RETAILER " \
                        " WHERE CITY_CODE ='" + city_code + "' and VALID_FLAG != 'N'"
                rec = hdb.select(s_sql)
                self.logger.info(" SQL=:\n%s",s_sql)
                all_df = pd.DataFrame(list(rec), columns=['R_KHTYBM', 'R_NAME', 'R_ADDRESS'])

                city_df = zy_err_df[zy_err_df['CITY_CODE']==city_code]
                for row in city_df.index:
                    r_prov_code = city_df.loc[row, 'PROV_CODE']
                    r_city_code = city_df.loc[row, 'CITY_CODE']
                    r_name = city_df.loc[row, 'R_NAME']
                    r_add = city_df.loc[row, 'R_ADDRESS']
                    r_code = city_df.loc[row, 'R_CODE']

                    for rtl_row in all_df.index:
                        rtl_name = all_df.loc[rtl_row, 'R_NAME']
                        rtl_add = all_df.loc[rtl_row, 'R_ADDRESS']
                        rtl_code = all_df.loc[rtl_row, 'R_KHTYBM']
                        if r_name == rtl_name:
                            arr_code.append([r_prov_code, r_city_code, r_code, rtl_code, rtl_name, rtl_add])
                            break
                        pass
                        if r_add == rtl_add:
                            arr_code.append([r_prov_code, r_city_code, r_code, rtl_code, rtl_name, rtl_add])
                            break
                        pass
                        sim = self.countSimilary(r_name + r_add,rtl_name + rtl_add)
                        if sim >= ratio:
                            arr_code.append([r_prov_code, r_city_code, r_code, rtl_code, rtl_name, rtl_add])
                            break
                        pass
                    pass
                pass
            pass
        except Exception as ex:
            self.logger.error(" 大数据平台数据库错误:\n%s",str(ex))
        finally:
            hdb.close()
        pass

        self.logger.info(" arr_code=%s",len(arr_code))
        match_df = pd.DataFrame(arr_code, columns=['PROV_CODE','CITY_CODE','R_CODE','R_KHTYBM','R_NAME_1','R_ADD_1'])
        rst_df = pd.merge(match_df, zy_err_df,  how='inner', on=['PROV_CODE','CITY_CODE','R_CODE'])
        self.logger.info(" rst_df=%s",len(rst_df))
        self.logger.info(" rst_df=%s",rst_df.columns)
        i_sql = ""
        tbl_match = tbl_3 + "_NA_MATCH"
        try:
            db.connect()
            # 0 获取核心户信息表
            s_sql = "SELECT PROV_CODE, CITY_CODE, R_CODE, R_KHTYBM " + \
                    " FROM  " + tbl_3  + \
                    " order by PROV_CODE, CITY_CODE"
            hxh_rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            hxh_df = pd.DataFrame(list(hxh_rec), columns=['PROV_CODE','CITY_CODE','R_CODE','R_KHTYBM'])
            # 0.1 去除核心户中已经存在的核心户
            tmp_df = pd.merge(hxh_df, rst_df,  how='inner', on=['PROV_CODE','CITY_CODE','R_CODE','R_KHTYBM'])
            rst_df = rst_df.append(tmp_df)
            rst_df.drop_duplicates(subset=['PROV_CODE', 'CITY_CODE', 'R_CODE','R_KHTYBM'], keep=False, inplace=True)
            self.logger.info(" rst_df=%s",len(rst_df))
            self.logger.info(" rst_df=%s",rst_df.columns)

            # 1 插入前先删除核心户信息错误表
            d_sql = "delete from " + tbl_match
            ret = db.delete(d_sql)
            self.logger.info(" RFM_RTL_HBZY_NA_MATCH删除成功= %s", corp_name + " - "  + str(ret))
            # 插入数据库
            PRE_SQL = "INSERT INTO " + tbl_match + "(BUSI_DATE, PROV_CODE,PROV_NAME, CITY_CODE,CITY_NAME,R_KHTYBM," \
                      " R_CODE,R_XKZH, R_NAME,R_ADDRESS,R_NAME_1,R_ADDRESS_1,DEPT_NAME,R_CONTACTOR,R_TEL, R_SLH," + \
                      " R_LABEL,PPCG) VALUES "
            i_sql = ""
            count = 0
            values = {'R_XKZH': '', 'R_NAME': '', 'R_ADDRESS': '', 'R_NAME_1': '', 'R_ADDRESS_1': '',
                      'DEPT_NAME': '', 'R_SLH': '', 'R_LABEL': '', 'R_CONTACTOR': '', 'R_TEL': ''}
            rst_df.fillna(value=values, inplace=True)
            rst_df.fillna(0, inplace=True)
            for row in rst_df.index:
                busi_date = rst_df.loc[row]['BUSI_DATE']
                prov_code = rst_df.loc[row]['PROV_CODE']
                pro_name = rst_df.loc[row]['PROV_NAME']
                city_code = rst_df.loc[row]['CITY_CODE']
                city_name = rst_df.loc[row]['CITY_NAME']
                r_khtym = rst_df.loc[row]['R_KHTYBM']
                r_code = rst_df.loc[row]['R_CODE']
                r_xkzh = rst_df.loc[row]['R_XKZH']
                r_name = rst_df.loc[row]['R_NAME']
                r_add = rst_df.loc[row]['R_ADDRESS']
                r_name_1 = rst_df.loc[row]['R_NAME_1']
                r_add_1 = rst_df.loc[row]['R_ADD_1']
                r_dept = rst_df.loc[row]['DEPT_NAME']
                r_slh = rst_df.loc[row]['R_SLH']
                r_label = rst_df.loc[row]['R_LABEL']
                r_contactor = rst_df.loc[row]['R_CONTACTOR']
                r_tel = rst_df.loc[row]['R_TEL']

                i_sql = i_sql + " ('" + busi_date + "', '" + prov_code + "', '" + pro_name + "', '" \
                        + city_code + "', '" + city_name + "', '" + r_khtym + "','"  + r_code + "', '" \
                        + r_xkzh + "', '" + r_name + "', '" + r_add + "','" + r_name_1 + "', '" + r_add_1 + "','" \
                        + r_dept + "', '" \
                        + r_contactor + "', '" + r_tel + "','"  + r_slh + "','"  + r_label + "', '0'),"
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
    model = RFMMatch()
    model.main()
pass
