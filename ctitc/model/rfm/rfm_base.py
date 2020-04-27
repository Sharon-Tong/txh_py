#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2019-08-04
#########################################################
import numpy as np
from ctitc.db.mysqldb import MysqlDB
from ctitc.common.log.mylog import MyLog
from ctitc.db.hivedb import HiveDB
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
#########################################################
# 价值客户筛选评估分析模型父类
#########################################################
class RFMBase():
    logger = None
    ##########################################
    # 初始化
    ##########################################
    def __init__(self, logkey="rfm"):
        self.logger = MyLog.getLogger(logkey, log_file='logger_rfm.conf')
    pass
    ##########################################
    # 从大数据平台获取零售户基本信息
    ##########################################
    def getRtlInfoFromHive(self, busi_date, tbl):
        # 1.0 从大数据平台获取信息
        hdb = HiveDB(logger=self.logger)
        all_df = None
        try:
            hdb.connect()
            s_sql = "SELECT BUSI_DATE, PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME, " \
                    " R_KHTYBM, R_KHBM, KHBH_DQ, R_XKZH, R_NAME,R_ADDRESS,R_JYYT,R_CXFL,R_DW,R_CONTACTOR,R_TEL," \
                    " R_NUM,R_RANK, F_NUM,F_RANK,M_NUM,M_RANK,X_NUM,X_RANK,ZJE,ZJE_RANK,ZDX,ZDX_RANK,LSH_GGS " \
                    " FROM  " + tbl + " WHERE BUSI_DATE='" + busi_date + "' order by PROV_CODE, CITY_CODE"
            rec = hdb.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            a_df = pd.DataFrame(list(rec), columns=['BUSI_DATE', 'PROV_CODE', 'PROV_NAME', 'CITY_CODE', 'CITY_NAME',
                                                    'R_KHTYBM', 'R_CODE', 'KHBH_DQ', 'R_XKZH','R_NAME', 'R_ADDRESS',
                                                    'R_JYYT', 'R_CXFL', 'R_DW', 'R_CONTACTOR','R_TEL', 'R_NUM','R_RANK',
                                                    'F_NUM', 'F_RANK', 'M_NUM','M_RANK', 'X_NUM','X_RANK',
                                                    'ZJE', 'ZJE_RANK', 'ZDX','ZDX_RANK','LSH_GGS'])
            all_df = a_df.loc[a_df['R_KHTYBM'].str[-4:] != 'AAAA']
            self.logger.info(" 大数据平台数据记录数,all_df=%s",len(all_df))
        except Exception as ex:
            self.logger.error(" 大数据平台数据库错误:\n%s",str(ex))
        finally:
            hdb.close()
        pass
        return all_df
    pass
    ##########################################
    # 从大数据平台获取零售户基本信息
    ##########################################
    def getRankFromHive(self, busi_date, tbl):
        # 1.0 从大数据平台获取信息
        hdb = HiveDB(logger=self.logger)
        start_date = self.getAddMonthByStep(busi_date,-2)
        end_date = busi_date
        rk_df = None
        try:
            hdb.connect()
            # 查询3个月数据
            s_sql = "SELECT PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME, R_KHTYBM," + \
                    " avg(R_NUM) R_NUM, sum(F_NUM) F_NUM, sum(M_NUM) M_NUM, sum(X_NUM) X_NUM," + \
                    " sum(ZJE) ZJE, sum(ZJE)/sum(ZJE/ZDX) ZDX, sum(LSH_GGS * 1.0) LSH_GGS " + \
                    " FROM  " + tbl + " WHERE BUSI_DATE>='" + start_date + "' and BUSI_DATE<='" + end_date + "' " + \
                    " and ZDX > 0.0 and prov_code='11110001' group by PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME, R_KHTYBM " + \
                    " order by PROV_CODE, CITY_CODE limit 10"
            rec = hdb.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            self.logger.info(" length=:%s",len(rec))
            a_df = pd.DataFrame(list(rec), columns=['PROV_CODE', 'PROV_NAME', 'CITY_CODE', 'CITY_NAME','R_KHTYBM',
                                                    'R_NUM', 'F_NUM', 'M_NUM', 'X_NUM', 'ZJE', 'ZDX','LSH_GGS'])
            rk_df = a_df.loc[a_df['R_KHTYBM'].str[-4:] != 'AAAA']
            self.logger.info(" rk_df=%s",len(rk_df))
            self.logger.info(" rk_df=%s",rk_df.columns)
            values = {'R_NUM': 9999999.9}
            rk_df.fillna(value=values, inplace=True)
            rk_df.fillna(0.0, inplace=True)
            # 排名
            self.logger.info(" rank_df=%s",rk_df.columns)
            # rank_df.to_excel("/home/rasmode/tensorflow/shell/rank.xlsx", index=False)
            rk_df['r_rank'] = rk_df['R_NUM'].groupby(rk_df['CITY_CODE']).rank(ascending=True,method='dense')
            # rk_df['F_RANK'] = rk_df['F_NUM'].groupby(rk_df['CITY_CODE']).rank(ascending=False,method='dense')
            # rk_df['M_RANK'] = rk_df['M_NUM'].groupby(rk_df['CITY_CODE']).rank(ascending=False,method='dense')
            # rk_df['X_RANK'] = rk_df['X_NUM'].groupby(rk_df['CITY_CODE']).rank(ascending=False,method='dense')
            # rk_df['ZJE_RANK'] = rk_df['ZJE'].groupby(rk_df['CITY_CODE']).rank(ascending=False,method='dense')
            # rk_df['ZDX_RANK'] = rk_df['ZDX'].groupby(rk_df['CITY_CODE']).rank(ascending=False,method='dense')
            self.logger.info(" rk_df=%s",rk_df.columns)

            # 查询零售户的属性
            prop_sql = "SELECT PROV_CODE, CITY_CODE, " \
                       " R_KHTYBM, R_KHBM, R_XKZH, R_NAME,R_ADDRESS,R_JYYT,R_CXFL,R_DW,R_CONTACTOR,R_TEL " + \
                       " FROM " + tbl + " WHERE BUSI_DATE>='" + start_date + "' and BUSI_DATE<='" + end_date + "' " + \
                       " and ZDX > 0.0 and prov_code='11110001' order by PROV_CODE, CITY_CODE"
            rec = hdb.select(prop_sql)
            self.logger.info(" SQL=:\n%s",prop_sql)
            prop_df = pd.DataFrame(list(rec), columns=['PROV_CODE', 'CITY_CODE','R_KHTYBM', 'R_CODE',
                                                       'R_XKZH','R_NAME', 'R_ADDRESS',
                                                       'R_JYYT', 'R_CXFL', 'R_DW', 'R_CONTACTOR','R_TEL'])
            prop_df.drop_duplicates(subset=['PROV_CODE', 'CITY_CODE', 'R_KHTYBM'], inplace=True)
            self.logger.info(" prop_df=%s",prop_df.columns)

            prop_df = prop_df.loc[prop_df['R_KHTYBM'].str[-4:] != 'AAAA']
            rk_df = pd.merge(rk_df, prop_df, how='left', on=['PROV_CODE','CITY_CODE','R_KHTYBM'])
            # self.logger.info(" rank_df=%s",rk_df.columns)
        except Exception as ex:
            self.logger.error(" 大数据平台数据库错误:\n%s",str(ex))
        finally:
            hdb.close()
        pass
        return rk_df
    pass
    ##########################################
    # 从大数据平台获取所有零售户基本信息
    ##########################################
    def getAllRtlInfoFromHive(self, city_codes=None):
        # 1.0 从大数据平台获取信息
        hdb = HiveDB(logger=self.logger)
        all_df = None
        try:
            hdb.connect()
            s_sql = "SELECT CITY_CODE, KHTYBM_DQ, R_NAME, R_ADDRESS " \
                    " FROM DIM_RETAILER WHERE CITY_CODE in " + city_codes + " and VALID_FLAG != 'N' order by CITY_CODE"
            rec = hdb.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            all_df = pd.DataFrame(list(rec), columns=['CITY_CODE', 'R_CODE', 'R_NAME', 'R_ADDRESS'])
        except Exception as ex:
            self.logger.error(" 大数据平台数据库错误:\n%s",str(ex))
        finally:
            hdb.close()
        pass
        return all_df
    pass
    ##########################################
    # 从大数据平台获取最大值
    ##########################################
    def getMaxValueFromHive(self, busi_date, tbl):
        # 1.0 从大数据平台获取信息
        hdb = HiveDB(logger=self.logger)
        max_df = None
        try:
            hdb.connect()
            s_sql = "SELECT CITY_CODE, max(R_RANK) R_RANK, max(F_RANK) F_RANK, max(M_RANK) M_RANK,  " \
                    " max(X_RANK) X_RANK, max(ZJE_RANK) ZJE_RANK,  max(ZDX_RANK) ZDX_RANK" \
                    " FROM " + tbl + " WHERE BUSI_DATE='" + busi_date + "' group by CITY_CODE order by CITY_CODE "
            rec = hdb.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            max_df = pd.DataFrame(list(rec), columns=['CITY_CODE', 'R_RANK','F_RANK','M_RANK', 'X_RANK',
                                                    'ZJE_RANK', 'ZDX_RANK'])
        except Exception as ex:
            self.logger.error(" 大数据平台数据库错误:\n%s",str(ex))
        finally:
            hdb.close()
        pass
        return max_df
    pass

    ##########################################
    # 从大数据平台获取零售户价值特征表(分地市)
    ##########################################
    def getCityFeatureFromHive(self, busi_date, tbl):
        # 1.0 从大数据平台获取信息
        hdb = HiveDB(logger=self.logger)
        zy_df = None
        all_df = None
        try:
            hdb.connect()
            # 自有零售户
            s_sql = "SELECT PROV_CODE, CITY_CODE, count(*) ZY_RTL_NUM, sum(F_NUM) F_NUM, " \
                    " sum(M_NUM) M_NUM, sum(X_NUM) X_NUM, (avg(F_NUM/LSH_GGS) / 3.0) HJGJCS " \
                    " FROM " + tbl + " WHERE BUSI_DATE='" + busi_date + "' AND X_NUM > 0.0 " \
                    " group by PROV_CODE, CITY_CODE order by PROV_CODE, CITY_CODE "
            rec = hdb.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            zy_df = pd.DataFrame(list(rec), columns=['PROV_CODE', 'CITY_CODE', 'ZY_RTL_NUM','F_NUM',
                                                     'M_NUM', 'X_NUM', 'HJGJCS'])

            # 全部零售户
            s_sql = "SELECT PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME, count(*) ALL_RTL_NUM, sum(ZJE) ZJE, " \
                    " (sum(ZJE / ZDX) / 40.0) ZXL " \
                    " FROM " + tbl + " WHERE BUSI_DATE='" + busi_date + "' AND ZJE > 0.0 " \
                    " group by PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME order by PROV_CODE, CITY_CODE "
            rec = hdb.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            all_df = pd.DataFrame(list(rec), columns=['PROV_CODE', 'PROV_NAME','CITY_CODE', 'CITY_NAME', 'ALL_RTL_NUM',
                                                     'ZJE', 'ZXL'])

        except Exception as ex:
            self.logger.error(" 大数据平台数据库错误:\n%s",str(ex))
        finally:
            hdb.close()
        pass
        # 关联处理
        rst_df = pd.merge(all_df, zy_df, how='left', on=['PROV_CODE','CITY_CODE'])
        rst_df.fillna(0, inplace=True)
        return rst_df
    pass

    #########################################
    # 对每一列数据进行统计，包括计数，均值，std，各个分位数等
    # 数据结构
    #           英语        经济数学
    # count   11.000000   11.000000
    # mean    82.727273   72.727273
    # std      8.649750   13.762928
    # min     70.000000   51.000000
    # 25%     77.000000   64.000000
    # 50%     83.000000   74.000000
    # 75%     88.000000   80.000000
    # max     97.000000   95.000000
    # cv       0.104557    0.189240
    # IQR     11.000000   16.000000
    # uline  104.500000  104.000000
    # dline   60.500000   40.000000
    ##########################################
    def getStatics(self, df=None):
        if df is None:
            return None
        pass
        # 1.获取列名
        # cols = df.columns
        param = df.describe()
        # 离散系数
        param.loc['cv'] = param.loc['std'] / abs(param.loc['mean'])
        # 中位线（IQR）：Q3-Q1上四分位数至下四分位数的距离
        param.loc['IQR'] = param.loc['75%'] - param.loc['25%']
        # 上限值：Q3+1.5×IQR
        param.loc['uline'] = param.loc['75%'] + param.loc['IQR']*1.5
        # 下限值：Q1-1.5×IQR
        param.loc['dline'] = param.loc['25%'] - param.loc['IQR']*1.5
        param.fillna(0.0, inplace=True)
        return param
    pass

    ##########################################
    # 计算2个字符串的相似度
    ##########################################
    def countSimilary(self,firstStr,secondStr):
        first_len = len(firstStr) * 1.0
        if first_len == 0.0:
            return 0.0
        pass
        second_len = len(secondStr) * 1.0
        for item in firstStr:
            secondStr = secondStr.replace(item,'',1)
        pass
        if first_len > second_len:
            ret = (second_len - len(secondStr) * 1.0) / first_len
        else:
            ret = (second_len - len(secondStr) * 1.0) / second_len
        pass
        return ret
    pass
    ###################################################################
    # 月份计算,增加step月
    ####################################################################
    def getAddMonthByStep(self, smonth, step):
        date = datetime.datetime.strptime(smonth,'%Y%m')
        delta = date + relativedelta(months=step)
        rtn = delta.strftime('%Y%m')
        return rtn
    pass

pass
if __name__ == "__main__":
    # def countSimilary(firstStr,secondStr):
    #     first_len = len(firstStr) * 1.0
    #     second_len = len(secondStr) * 1.0
    #     for item in firstStr:
    #         tmp = secondStr.replace(item,'',1)
    #         print(tmp)
    #         secondStr = tmp
    #     pass
    #     ret = (second_len - len(secondStr) * 1.0) / first_len
    #     return ret
    # pass
    # tx1 = '城关\镇\阜蒙路'
    # tx1 = repr(tx1).replace('\\','')
    # print(tx1)
    # percent = ['FACTOR_XQMZL','FACTOR_XLZB','FACTOR_XLJZD','FACTOR_SCFE','FACTOR_XPXLZB','FACTOR_PHL',
    #            'FACTOR_CGL','FACTOR_DGL','FACTOR_LSHSB','FACTOR_HJXLB','FACTOR_DGM','FACTOR_DZM',
    #            'FACTOR_XHZS','FACTOR_DZL']
    # if "FACTOR_XQMZL" in percent:
    #     print("ok")
    fields="FACTOR_XQMZL=0.92&FACTOR_DGL=12.&FACTOR_LSHSB="
    dic_item = {}
    if fields is None or fields == "":
        pass
    else:
        # 解析fields,格式为key=value&key=value
        params = str.split(fields, sep="&")
        for param in params:
            item = str.split(param, sep="=")
            dic_item[item[0]] = item[1]
        pass
    pass
    print(dic_item)
    print(dic_item['FACTOR_XQMZL'])
    # rfm = RFMBase()
    # rfm.getRankFromHive('201901','usertmp.RFM_RTL_FEATURE_HBZY')
    # ret = countSimilary(tx, tx1)
    # print(ret)
pass