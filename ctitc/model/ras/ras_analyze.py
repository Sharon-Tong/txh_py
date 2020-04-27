#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2017-08-31
#########################################################
import numpy as np
from com.ctitc.bigdata.db.mysqldb import MysqlDB
from com.ctitc.bigdata.model.ras.ras_base import RASBase
import pandas as pd
from sklearn.linear_model import LinearRegression
from com.ctitc.bigdata.db.hivedb import HiveDB

import warnings
import argparse
import os
from com.ctitc.bigdata.util.dateutil import DateUtil
########################################################
# 市场健康状态评价分析及模拟仿真分析模型（RAS）
# 市场健康度分析是基于市场健康度得分，结合营销各环节，
# 从不同维度对市场进行全面综合分析.
# 处理步骤:
# 1)获取源数据,2)数据预处理,3)源数据计算二级指标的各统计变量:max,min,75%,cv等,
# 4)标准差计算标准值,二级指标得分=cv的权重*标准值,一级指标得分=cv的权重*标准值,
# 5) 二级指标的各统计变量:max,min,75%,cv来源于二级指标源数据,
# 6) 一级指标的各统计变量:max,min,75%,cv来源于一级指标得分数据
# 7) 二级指标源数据与评分结果,进行多元线性回归,选择变量.
#########################################################
class RASAnalyze(RASBase):
    ##########################################
    # 因子分析模型初始化
    ##########################################
    def __init__(self, action_type='1'):
        super().__init__(logkey="ras_ana",action_type=action_type)
        self.cur_data = None
    pass
    ##########################################
    # 主处理函数
    ##########################################
    def main(self):
        # 1.初始化数据库, 并查询数据
        db = MysqlDB(logger=self.logger)
        barcodes = None
        try:
            db.connect()
            # 1.查询需要处理的规格及规格组别代码
            s_total = "select BAR_CODE,BAR_NAME, GROUP_NAME, GROUP_CODE from RAS_BAR_CONFIG " \
                      " where IS_USE = '" + self.action_type + "' order by  GROUP_CODE, BAR_CODE "
            barcodes = db.select(s_total)
        finally:
            db.close()
        pass
        if len(barcodes) <= 0 :
            self.logger.error("record num is 0! exit system.")
            exit("record num is 0! exit system.")
        pass
        grp_flg = ""
        d_1 = []
        d_2 = []
        d_3 = []
        d_4 = []
        arr_d_1 = []
        arr_d_2 = []
        arr_d_3 = []
        arr_d_4 = []
        for barcode in barcodes:
            bar_code = barcode[0]
            bar_name = barcode[1]
            grp_name = barcode[2]
            grp_code = barcode[3]
            if grp_code == grp_flg:
                d_1.append(bar_code)
                d_2.append(bar_name)
                d_3.append(grp_name)
                d_4.append(grp_code)
                grp_flg = grp_code
            else:
                if (grp_flg != "") :
                    arr_d_1.append(d_1)
                    arr_d_2.append(d_2)
                    arr_d_3.append(d_3)
                    arr_d_4.append(d_4)
                    d_1 = []
                    d_2 = []
                    d_3 = []
                    d_4 = []
                pass
                d_1.append(bar_code)
                d_2.append(bar_name)
                d_3.append(grp_name)
                d_4.append(grp_code)
                grp_flg = grp_code
            pass
        pass
        arr_d_1.append(d_1)
        arr_d_2.append(d_2)
        arr_d_3.append(d_3)
        arr_d_4.append(d_4)

        self.logger.info("===========================START Analyze====================")
        cnt = 0
        for item in arr_d_1:
            group_code = arr_d_4[cnt][0]
            group_msg = arr_d_3[cnt][0] + "(" + group_code + ")"
            self.logger.info("开始任务处理:%s",group_msg)
            self.logger.info("正在处理的规格:%s",",".join(arr_d_2[cnt]) + "(" + ",".join(item) + ")")
            # print("\n正在处理的规格%s" % (item))
            try:
                # 1. 获取计算指标及所需的数据源
                self.logger.info("1.开始获取计算指标及所需的数据源...")
                first_fts, fts, data1 = self.getCVData(item, group_code)
                data_cp = data1.copy(deep=True)
                # 2. 数据预处理
                self.logger.info("2.开始数据预处理...")
                data = self.preProcessData(group_code, data1,'0')
                # 3.计算CV值及得分
                self.logger.info("3.开始计算CV值及得分...")
                self.countCV(group_code,'0',data,first_fts,fts)
                # 4.通过逐步回归选择可调整变量
                self.logger.info("4.开始通过逐步回归选择可调整变量...")
                self.proMultiRegData(data_cp, group_code, fts)
            except Exception as ex:
                self.logger.error(str(ex))
                self.logger.info("任务处理异常:%s",group_msg)
            pass
            cnt += 1
            self.logger.info("结束任务处理:%s",group_msg)
        pass
        self.logger.info("===========================END Analyze====================")
    pass

    ##########################################
    # 获取计算指标CV值及得分所需的数据源
    ##########################################
    def getCVData(self, bars=None, group_code=None):
        # 1.0 初始化数据库, 并查询处理的规格
        sql_condition = ""
        cnt = 0
        for key in bars:
            if sql_condition == "":
                sql_condition = "'" + key + "' "
            else:
                sql_condition += ",'" + key + "' "
            pass
            cnt += 1
        pass
        # 1.1.获取特征值
        # 一级特征
        first_fts = self.getFirstFeatures(group_code)
        # 全部特征
        fts = self.getFeatures(group_code)
        sql_ft = ""
        pd_cols = ""
        for ft in fts:
            group_code = ft[0]
            first_code = ft[1]
            first_name = ft[2]
            second_code = ft[3]
            second_name = ft[4]
            if sql_ft == "" :
                sql_ft = second_code
            else:
                sql_ft = sql_ft +  ", "  + second_code
            pass
            pd_cols = pd_cols + ", '" + second_code + "'"
        pass

        # 1.2. 查询源数据
        # 1.2.0.获取当前月份
        cur_ym = DateUtil.get_nowym()
        # 原始数据源
        data_old = None
        # 处理后的数据源
        data = None
        db = HiveDB(logger=self.logger)
        try:
            db.connect()
            # 1.查询需要处理的规格
            # s_sql = "select distinct CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, BUSI_DATE,  "  + sql_ft + " " \
            #          " from RAS_FT_SRC_CITY_MONTH " \
            #          "  WHERE FACTOR_XL > 0.0 and BAR_CODE in (" + sql_condition  + ") and BUSI_DATE = '201908' " + \
            #         " and city_code in('11420101','11320101') " \
            #         " order by  BAR_CODE, CITY_CODE, BUSI_DATE "
            s_sql = "select CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, BUSI_DATE,  "  + sql_ft + \
                    " from RAS_FT_SRC_CITY_MONTH " +\
                    "  WHERE FACTOR_XL > 0.0 and BUSI_DATE < '" + cur_ym + "'" +\
                    " and BAR_CODE in (" + sql_condition  + ") " +\
                    " order by  BAR_CODE, CITY_CODE, BUSI_DATE "
            rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            cols = "['CITY_CODE', 'CITY_NAME', 'BAR_CODE', 'BAR_NAME', 'BUSI_DATE'" + pd_cols + "]"
            data = pd.DataFrame(list(rec), columns=eval(cols))
            self.cur_data = pd.DataFrame(list(rec), columns=eval(cols))
            # 去重,是不是与LEVEL_CODE相关?,2019-11-23
            data.drop_duplicates(subset=['CITY_CODE', 'BAR_CODE', 'BUSI_DATE'],inplace=True)
            self.cur_data.drop_duplicates(subset=['CITY_CODE', 'BAR_CODE', 'BUSI_DATE'],inplace=True)
            # data.fillna(0.0, inplace=True)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
        finally:
            db.close()
        pass
        if len(self.cur_data) <= 0:
            return None
        pass
        return first_fts, fts, data
    pass
    ##########################################
    # 数据预处理
    # 反向指标和缺失值处理
    # 0:缺失值为1,1:删除缺失值
    ##########################################
    def preProcessData(self, group_code='', data=None, pro_type='0'):
        if data is None:
            return None
        pass
        # 1.0 缺失值处理
        # 价格指数(FACTOR_JGZS):用“1”代替
        # 社会存销比(FACTOR_SHCXB),商业存销比(FACTOR_SYCXB):平均值
        cols = data.columns
        for col in cols:
            if col == 'FACTOR_JGZS':
                data[col].fillna(1.0,inplace = True)
            pass
            if col == 'FACTOR_SHCXB' or col == 'FACTOR_SYCXB':
                data[col].fillna(data[col].mean(),inplace = True)
            pass
            if col == 'FACTOR_XQMZL':
                data[col] = data[col].apply(lambda x: 100.0 if x > 100.0 else x)
            pass
        pass

        # 2.0 反向指标处理
        # 需求满足率(FACTOR_XQMZL),销量排名(FACTOR_XLPM),商业存销比(FACTOR_SYCXB)
        # 社会存销比(FACTOR_SHCXB),销量集中度(FACTOR_XLJZD)
        reverse = self.getReverseFeatures(group_code)
        self.logger.info("reverse:\n%s",reverse)
        self.logger.info("cols:\n%s",cols)
        for col in cols:
            if col in reverse:
                data[col] = data[col].apply(lambda x: 1.0/x if x != 0.0 else x)
            pass
        pass

        # 3.0 其余缺失值处理
        data.fillna(0.0, inplace=True)
        # if pro_type =='0':
        #     data.fillna(0.0, inplace=True)
        # else:
        #     data.dropna(axis=0,how='all',inplace=True)
        # pass

        # 4.0 异常值处理处理, 按地市/省份进行异常值处理
        # codes_df = data[['CITY_CODE','CITY_NAME']]
        # city_codes = codes_df.drop_duplicates(subset=['CITY_CODE','CITY_NAME'], inplace=False)
        # for row in city_codes.index:
        #     city_code = city_codes.loc[row,'CITY_CODE']
        #     city_name = city_codes.loc[row,'CITY_NAME']
        #     city_df = data[data['CITY_CODE']==city_code]
        #     city_stat = self.getStatics(city_df.drop(city_df.columns[0:5], axis=1, inplace=False))
        #
        #     # stat = self.getStatics(data.drop(data.columns[0:5], axis=1, inplace=False))
        #     # self.logger.info("city_name=:%s",city_code + city_name)
        #     # self.logger.info("原始数据分布:\n%s",city_stat)
        #     # 3.1 异常值处理
        #     cols = city_stat.columns
        #     for col in cols:
        #         up_value = city_stat.loc['uline',col]
        #         down_value = city_stat.loc['dline',col]
        #         data.loc[data.CITY_CODE == city_code, col] = \
        #             data.loc[data.CITY_CODE == city_code, col].apply(lambda x: up_value if x >= up_value else down_value if x <= down_value else x)
        #     pass
        # pass

        if pro_type =='0':
            stat = self.getStatics(data.drop(data.columns[0:5], axis=1, inplace=False))
            self.logger.info("原始数据分布:\n%s",stat)
            # 3.1 异常值处理
            cols = stat.columns
            for col in cols:
                up_value = stat.loc['uline',col]
                down_value = stat.loc['dline',col]
                data[col] = data[col].apply(lambda x: up_value if x >= up_value else down_value if x <= down_value else x)
            pass
        pass
        return data
    pass

    ##########################################
    # 计算指标CV值及得分，以及各指标区间
    ##########################################
    def countCV(self, group_code=None, pro_code='0', data=None, first_fts=None, fts=None):

        stat_adj = self.getStatics(data.drop(data.columns[0:5], axis=1, inplace=False))
        cols = stat_adj.columns
        self.logger.info("调整后的数据指标项:\n%s",cols)

        # 3.二级指标权重/得分计算
        db = MysqlDB(logger=self.logger)
        i_sql = ""
        try:
            db.connect()
            # 3.1 计算指标权重表
            # 3.1.0 插入指标权重表
            # 3.1.1 插入前先删除指标权重表
            d_sql = "delete from RAS_FT_WEIGHT_MONTH where GROUP_CODE='" + group_code + "'"
            db.delete(d_sql)
            count = 0
            PRE_SQL = "INSERT INTO RAS_FT_WEIGHT_MONTH (GROUP_CODE, FIRST_FT_CODE, SECOND_FT_CODE, " \
                      " CV_VALUE, CV_FIRST_WEIGHT,  CV_SECOND_WEIGHT) VALUES "
            # 3.2 一/二级指标求和
            dic_first = {}
            dic_code = {}
            code_flg = ""
            # 一级指标的cv
            sum_value = 0.0
            # 所有指标的cv
            total_value = 0.0
            for ft in fts:
                first_code = ft[1]
                second_code = ft[3]
                cv_value = stat_adj.loc['cv',second_code]
                dic_code[second_code] = first_code
                if first_code == code_flg:
                    sum_value = sum_value + cv_value
                else:
                    if code_flg != "":
                        dic_first[code_flg] = sum_value
                        sum_value = 0.0
                    pass
                    sum_value = sum_value + cv_value
                pass
                code_flg = first_code
                total_value = total_value + cv_value
            pass
            # 最后一个
            dic_first[code_flg] = sum_value

            # 一/二级权重
            count = 0
            for ft in fts:
                group_code = ft[0]
                first_code = ft[1]
                first_name = ft[2]
                second_code = ft[3]
                second_name = ft[4]
                cv_value = stat_adj.loc['cv',second_code]
                cv_first_value  = cv_value / total_value
                cv_second_value = cv_value / dic_first[first_code]

                count = count + 1
                i_sql = i_sql + " ('" + group_code + "', '" + first_code + "', " +\
                             " '" + second_code + "', "  + str(cv_value)+ "," +\
                              str(cv_first_value) + ", "  +  str(cv_second_value)+ " ),"
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                # self.logger.info("  i_sql= :%s", i_sql)
                ret = db.insert(i_sql)
            pass
            self.logger.info(" 成功插入指标权重月表(RAS_FT_WEIGHT_MONTH)记录数 = :%s",count)

            # 3.2 计算指标得分
            # 3.2.0-1 获取指标权重月表(RAS_FT_WEIGHT_MONTH)
            s_sql = "SELECT GROUP_CODE, FIRST_FT_CODE, SECOND_FT_CODE, CV_VALUE, CV_FIRST_WEIGHT,  " \
                    " CV_SECOND_WEIGHT FROM RAS_FT_WEIGHT_MONTH WHERE GROUP_CODE='" + group_code + "'"
            w_rec = db.select(s_sql)
            self.logger.info(" 指标权重月表 SQL=:\n%s",s_sql)
            w_df = pd.DataFrame(list(w_rec), columns=['GROUP_CODE', 'FIRST_FT_CODE','SECOND_FT_CODE',
                                                             'CV_VALUE','CV_FIRST_WEIGHT','CV_SECOND_WEIGHT'])

            # 3.2.0 插入指标得分表
            # 3.2.1 插入前先删除指标得分表
            d_sql = "delete from RAS_FT_SCORE_MONTH where GROUP_CODE='" + group_code + "' "
            db.delete(d_sql)
            count = 0
            index = 0
            PRE_SQL = "INSERT INTO RAS_FT_SCORE_MONTH (GROUP_CODE, BUSI_DATE, PRO_CODE, CITY_CODE, CITY_NAME, " \
                      " BAR_CODE, BAR_NAME, Z_SCORE, FIRST_FT_CODE, FIRST_FT_SCORE, SECOND_FT_CODE,  " \
                      " SECOND_FT_SCORE, SECOND_VALUE, SECOND_TRAN_VALUE, SECOND_STD_VALUE) VALUES "
            i_sql = ""
            # data:去掉异常值后的dataframe
            # self.cur_rec:原始数据,未出掉异常值
            self.logger.info(" data原始数据记录条数=:\n%s",len(data))
            for row in data.index:
                city_code = data.loc[row,'CITY_CODE']
                city_name = data.loc[row,'CITY_NAME']
                bar_code = data.loc[row,'BAR_CODE']
                bar_name = data.loc[row,'BAR_NAME']
                busi_date = data.loc[row,'BUSI_DATE']

                # 计算总分
                z_score = 0.0
                for col in cols:
                    max = stat_adj.loc['max',col]
                    min = stat_adj.loc['min',col]
                    mean = stat_adj.loc['mean',col]
                    z_weight = w_df[w_df['SECOND_FT_CODE'] == col]['CV_FIRST_WEIGHT'].values[0]
                    if (max - min) != 0.0 :
                        z_value = (data.loc[row, col] - mean) /(max - min)
                        z_score = z_score + (z_value * 100.0 + 60.0) * z_weight
                    else:
                        # first_score = "null"
                        pass
                    pass
                pass

                # col_count = 1
                first_flg = ""
                first_score = 0.0
                for col in cols:
                    max = stat_adj.loc['max',col]
                    min = stat_adj.loc['min',col]
                    mean = stat_adj.loc['mean',col]
                    std = stat_adj.loc['std',col]
                    # 需要讨论??
                    # ft_value = (data.loc[row][col] - min) /(max - min)

                    first_code = dic_code[col]
                    if first_code == first_flg:
                        first_flg = first_code
                    else:
                        first_arr = w_df[w_df['FIRST_FT_CODE'] == first_code]['SECOND_FT_CODE'].values
                        first_score = 0.0
                        for f_col in first_arr:
                            f_weight = w_df[w_df['SECOND_FT_CODE'] == f_col]['CV_SECOND_WEIGHT'].values[0]
                            f_mean = stat_adj.loc['mean',f_col]
                            f_std = stat_adj.loc['std',f_col]
                            f_max = stat_adj.loc['max',f_col]
                            f_min = stat_adj.loc['min',f_col]
                            if (f_max - f_min) != 0.0 :
                                f_value = (data.loc[row, f_col] - f_min) /(f_max - f_min)
                                s_mean = (f_mean - f_min) /(f_max - f_min)
                                first_score = first_score + ((f_value - s_mean) * 100.0 + 60.0) * f_weight
                            else:
                                # first_score = "null"
                                pass
                            pass
                        pass
                        first_flg = first_code
                    pass

                    second_score = 0.0
                    # second_weight = w_df[w_df['SECOND_FT_CODE'] == col]['CV_SECOND_WEIGHT'].values[0]
                    ft_value = 0.0

                    if (max - min) != 0.0 :
                        ft_value = (data.loc[row, col] - min) /(max - min)
                        # first_score = ft_value * first_weight
                        s_mean = (mean - min)/(max - min)
                        second_score = (ft_value - s_mean) * 100.0 + 60.0
                    else:
                        ft_value = "null"
                        second_score = "null"
                    pass
                    t_value = self.cur_data[(self.cur_data['CITY_CODE']==city_code) &
                                            (self.cur_data['BAR_CODE']==bar_code) &
                                            (self.cur_data['BUSI_DATE']==busi_date)][col].values[0]
                    # t_value = self.cur_rec[index][4 + col_count]
                    if t_value is None  or str(t_value) == "nan" or str(t_value) == "NaN":
                        t_value = "null"
                    pass
                    trans_value = data.loc[row, col]
                    if trans_value is None  or str(trans_value) == "nan" or str(trans_value) == "NaN":
                        trans_value = "null"
                    pass
                    count = count + 1
                    i_sql = i_sql + " ('" + group_code + "', '" +  busi_date + "', '" +  pro_code + "', '" + city_code + "', " \
                                  " '" + city_name + "', '" + bar_code + "', '" + bar_name + "', " + str(z_score) + ",'" +\
                                  dic_code[col] + "'," + \
                                  str(first_score) + ",'" + col + "',"  + str(second_score) + ", "  + str(t_value) + ", " +\
                                  str(trans_value) + "," + str(ft_value) + " ),"
                    if count%5000 == 0:
                        i_sql = i_sql.rstrip(',')
                        i_sql = PRE_SQL + i_sql
                        ret = db.insert(i_sql)
                        i_sql = ""
                    pass
                    # col_count = col_count + 1
                pass
                index = index + 1
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info(" 成功插入指标得分月表(RAS_FT_SCORE_MONTH)记录数 = :%s",count)

            # 3.2.4 计算一级指标/二级指标区间
            # 模拟时,不再重新计算区间值
            if (pro_code == '0'):
                self.logger.info(" 开始计算月度一级指标/二级指标区间...")
                # 1.0 查询反向指标
                reverse_ft = self.getReverseFeatures(group_code)
                # 1.1查询一级指标值
                s_sql = "select distinct GROUP_CODE, BUSI_DATE, CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, " \
                        " FIRST_FT_CODE, FIRST_FT_SCORE from RAS_FT_SCORE_MONTH  " \
                        " where GROUP_CODE='" + group_code + "' and PRO_CODE='0' " \
                        " order by FIRST_FT_CODE, CITY_CODE, BAR_CODE "
                first_rec = db.select(s_sql)
                self.logger.info(" 一级指标 SQL=:\n%s",s_sql)
                first_pd = pd.DataFrame(list(first_rec), columns=['GROUP_CODE', 'BUSI_DATE', 'CITY_CODE', 'CITY_NAME',
                                                                  'BAR_CODE', 'BAR_NAME', 'FIRST_FT_CODE', 'FIRST_FT_SCORE'])
                first_pd.fillna(0.0, inplace=True)

                # 1.2查询二级指标值
                s_sql = "select GROUP_CODE, BUSI_DATE, CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, " \
                        " SECOND_FT_CODE, SECOND_FT_SCORE from RAS_FT_SCORE_MONTH  " \
                        " where GROUP_CODE='" + group_code + "' and PRO_CODE='0' " \
                        " order by FIRST_FT_CODE, SECOND_FT_CODE, CITY_CODE, BAR_CODE "
                second_rec = db.select(s_sql)
                self.logger.info(" 二级指标 SQL=:\n%s",s_sql)
                second_pd = pd.DataFrame(list(second_rec), columns=['GROUP_CODE', 'BUSI_DATE', 'CITY_CODE', 'CITY_NAME',
                                        'BAR_CODE', 'BAR_NAME', 'SECOND_FT_CODE', 'SECOND_FT_SCORE'])
                second_pd.fillna(0.0, inplace=True)

                # 二级指标--全国
                # 3.2.4.0 插入全国的月度指标区间表
                # 3.2.4.1 插入前先删除全国的月度指标区间表
                d_sql = "delete from RAS_FT_SCOPE_COUNTRY_MONTH where GROUP_CODE='" + group_code + "'"
                db.delete(d_sql)
                # 3.2.4.3 插入前先删除地市的月度指标区间
                d_sql = "delete from RAS_FT_SCOPE_CITY_MONTH where GROUP_CODE='" + group_code + "'"
                db.delete(d_sql)

                count = 0
                PRE_SQL_COUNTRY = "INSERT INTO RAS_FT_SCOPE_COUNTRY_MONTH (GROUP_CODE, FIRST_FT_CODE, FIRST_FT_MEAN," \
                            "  FIRST_FT_STDEV, FIRST_FT_MAX,  FIRST_FT_MIN, FIRST_FT_DLINE, FIRST_FT_ULINE, " \
                            " SECOND_FT_CODE, SECOND_FT_MEAN, SECOND_FT_STDEV," \
                          " SECOND_FT_DLINE,SECOND_FT_ULINE, SECOND_FT_MAX, SECOND_FT_MIN, SECOND_FT_MEAN_VIEW, "  \
                          " SECOND_FT_DLINE_VIEW,SECOND_FT_ULINE_VIEW, SECOND_FT_MAX_VIEW, SECOND_FT_MIN_VIEW,  SECOND_FT_SCORE_MEAN, "  \
                          " SECOND_FT_SCORE_STDEV, SECOND_FT_SCORE_DLINE, SECOND_FT_SCORE_ULINE, SECOND_FT_SCORE_MAX, " \
                          " SECOND_FT_SCORE_MIN) VALUES "
                PRE_SQL_CITY = "INSERT INTO RAS_FT_SCOPE_CITY_MONTH (GROUP_CODE, CITY_CODE, CITY_NAME, " \
                          " FIRST_FT_CODE,FIRST_FT_MEAN, FIRST_FT_STDEV, FIRST_FT_MAX, FIRST_FT_MIN, FIRST_FT_DLINE, " \
                          " FIRST_FT_ULINE, SECOND_FT_CODE, SECOND_FT_MEAN, SECOND_FT_STDEV, SECOND_FT_DLINE," \
                       " SECOND_FT_ULINE, SECOND_FT_MAX, SECOND_FT_MIN, SECOND_FT_MEAN_VIEW, SECOND_FT_DLINE_VIEW, " \
                       " SECOND_FT_ULINE_VIEW, SECOND_FT_MAX_VIEW, SECOND_FT_MIN_VIEW, SECOND_FT_SCORE_MEAN, SECOND_FT_SCORE_STDEV, " \
                       " SECOND_FT_SCORE_DLINE, SECOND_FT_SCORE_ULINE, SECOND_FT_SCORE_MAX, SECOND_FT_SCORE_MIN) VALUES "
                i_country_sql = ""
                for ft in fts:
                    group_code = ft[0]
                    first_code = ft[1]
                    first_name = ft[2]
                    second_code = ft[3]
                    second_name = ft[4]
                    # 一级指标
                    f_col_df = first_pd[first_pd['FIRST_FT_CODE']==first_code]
                    f_col_stat = self.getStatics(f_col_df.drop(f_col_df.columns[0:7], axis=1, inplace=False))
                    # f_col_stat.fillna(0.0, inplace=True)
                    f_max = f_col_stat.loc['max','FIRST_FT_SCORE']
                    f_min = f_col_stat.loc['min','FIRST_FT_SCORE']
                    f_mean = f_col_stat.loc['mean','FIRST_FT_SCORE']
                    f_stdev = f_col_stat.loc['std','FIRST_FT_SCORE']
                    f_dline = f_col_stat.loc['50%','FIRST_FT_SCORE']
                    f_uline = f_col_stat.loc['75%','FIRST_FT_SCORE']

                    # 二级指标
                    ft_col_df = second_pd[second_pd['SECOND_FT_CODE']==second_code]
                    ft_col_stat = self.getStatics(ft_col_df.drop(ft_col_df.columns[0:7], axis=1, inplace=False))
                    # ft_col_stat.fillna(0.0, inplace=True)
                    s_max = ft_col_stat.loc['max','SECOND_FT_SCORE']
                    s_min = ft_col_stat.loc['min','SECOND_FT_SCORE']
                    s_mean = ft_col_stat.loc['mean','SECOND_FT_SCORE']
                    s_stdv = ft_col_stat.loc['std','SECOND_FT_SCORE']
                    s_dline = ft_col_stat.loc['50%','SECOND_FT_SCORE']
                    s_uline = ft_col_stat.loc['75%','SECOND_FT_SCORE']

                    ft_dline = stat_adj.loc['50%',second_code]
                    ft_uline = stat_adj.loc['75%',second_code]
                    max = stat_adj.loc['max',second_code]
                    min = stat_adj.loc['min',second_code]
                    mean = stat_adj.loc['mean',second_code]
                    stdv = stat_adj.loc['std',second_code]

                    view_max = max
                    view_min = min
                    view_mean = mean
                    view_uline = ft_uline
                    view_dline = ft_dline
                    if second_code in reverse_ft:
                        if min != 0.0:
                            view_max = 1.0/min
                        pass
                        if max != 0.0:
                            view_min = 1.0/max
                        pass
                        if mean != 0.0:
                            view_mean = 1.0/mean
                        pass
                        if ft_uline != 0.0:
                            view_dline = 1.0/ft_uline
                        pass
                        if ft_dline != 0.0:
                            view_uline = 1.0/ft_dline
                        pass
                    pass
                    count = count + 1
                    i_country_sql = i_country_sql + " ('" + group_code + "', '" +  first_code + "'," \
                            + str(f_mean) + "," + str(f_stdev) + "," + str(f_max) + "," \
                            + str(f_min) + "," + str(f_dline) + "," + str(f_uline) + ", '" + second_code + "', " \
                            + str(mean)+ "," + str(stdv) + "," + str(ft_dline) + ","  +  str(ft_uline) + "," \
                            + str(max) + "," + str(min) + "," + str(view_mean)+ "," + str(view_dline) + "," \
                            + str(view_uline) + "," + str(view_max) + "," + str(view_min)+ "," \
                            + str(s_mean)+ "," + str(s_stdv) + "," \
                            + str(s_dline) + ","  +  str(s_uline) + "," + str(s_max) + "," + str(s_min) + "),"
                pass
                if (i_country_sql != ""):
                    i_country_sql = i_country_sql.rstrip(',')
                    i_country_sql = PRE_SQL_COUNTRY + i_country_sql
                    ret = db.insert(i_country_sql)
                pass
                self.logger.info(" 成功插入全国的月度指标区间表(RAS_FT_SCOPE_COUNTRY_MONTH)记录数 = :%s",count)

                # 二级指标--地市
                # 3.2.4.2 插入地市的月度指标区间
                count = 0
                i_sql = ""
                citycodes = self.getCityCodes(group_code, pro_code, 'RAS_FT_SCORE_MONTH')
                for row in citycodes:
                    city_code = row[0]
                    city_name = row[1]
                    city_df = data[data['CITY_CODE']==city_code]
                    city_stat = self.getStatics(city_df.drop(city_df.columns[0:5], axis=1, inplace=False))
                    # city_stat.fillna(0.0, inplace=True)
                    f_city_df = first_pd[first_pd['CITY_CODE']==city_code]

                    s_city_df = second_pd[second_pd['CITY_CODE']==city_code]
                    for ft in fts:
                        group_code = ft[0]
                        first_code = ft[1]
                        first_name = ft[2]
                        second_code = ft[3]
                        second_name = ft[4]

                        # 一级指标
                        f_col_df = f_city_df[f_city_df['FIRST_FT_CODE']==first_code]
                        f_col_stat = self.getStatics(f_col_df.drop(f_col_df.columns[0:7], axis=1, inplace=False))
                        # f_col_stat.fillna(0.0, inplace=True)
                        f_max = f_col_stat.loc['max','FIRST_FT_SCORE']
                        f_min = f_col_stat.loc['min','FIRST_FT_SCORE']
                        f_mean = f_col_stat.loc['mean','FIRST_FT_SCORE']
                        f_stdev = f_col_stat.loc['std','FIRST_FT_SCORE']
                        f_dline = f_col_stat.loc['50%','FIRST_FT_SCORE']
                        f_uline = f_col_stat.loc['75%','FIRST_FT_SCORE']

                        # 二级指标
                        ft_col_df = s_city_df[s_city_df['SECOND_FT_CODE']==second_code]
                        ft_col_stat = self.getStatics(ft_col_df.drop(ft_col_df.columns[0:7], axis=1, inplace=False))
                        # ft_col_stat.fillna(0.0, inplace=True)
                        s_max = ft_col_stat.loc['max','SECOND_FT_SCORE']
                        s_min = ft_col_stat.loc['min','SECOND_FT_SCORE']
                        s_mean = ft_col_stat.loc['mean','SECOND_FT_SCORE']
                        s_stdv = ft_col_stat.loc['std','SECOND_FT_SCORE']
                        s_dline = ft_col_stat.loc['50%','SECOND_FT_SCORE']
                        s_uline = ft_col_stat.loc['75%','SECOND_FT_SCORE']

                        ft_dline = city_stat.loc['50%',second_code]
                        ft_uline = city_stat.loc['75%',second_code]
                        max = city_stat.loc['max',second_code]
                        min = city_stat.loc['min',second_code]
                        mean = city_stat.loc['mean',second_code]
                        stdv = city_stat.loc['std',second_code]

                        view_max = max
                        view_min = min
                        view_mean = mean
                        view_uline = ft_uline
                        view_dline = ft_dline
                        if second_code in reverse_ft:
                            if min != 0.0:
                                view_max = 1.0/min
                            pass
                            if max != 0.0:
                                view_min = 1.0/max
                            pass
                            if mean != 0.0:
                                view_mean = 1.0/mean
                            pass
                            if ft_uline != 0.0:
                                view_dline = 1.0/ft_uline
                            pass
                            if ft_dline != 0.0:
                                view_uline = 1.0/ft_dline
                            pass
                        pass
                        count = count + 1
                        i_sql = i_sql + " ('" + group_code + "','" + city_code +"','" + city_name + "', '" \
                                + first_code + "'," + str(f_mean) + "," + str(f_stdev) + "," + str(f_max) + ","\
                                + str(f_min) + "," + str(f_dline) + "," + str(f_uline) + ", '" + second_code + "', " \
                                + str(mean) + "," + str(stdv)+ "," + str(ft_dline)+ ","  +  str(ft_uline) + ","\
                                + str(max) + "," + str(min) + "," + str(view_mean)+ "," + str(view_dline) + "," \
                                + str(view_uline) + "," + str(view_max) + "," + str(view_min)+ "," \
                                + str(s_mean)+ "," + str(s_stdv) + "," \
                                + str(s_dline) + ","  +  str(s_uline) + "," + str(s_max) + "," + str(s_min) + "),"
                        if count%2000 == 0:
                            i_sql = i_sql.rstrip(',')
                            i_sql = PRE_SQL_CITY + i_sql
                            ret = db.insert(i_sql)
                            i_sql = ""
                        pass
                    pass
                pass
                if (i_sql != ""):
                    i_sql = i_sql.rstrip(',')
                    i_sql = PRE_SQL_CITY + i_sql
                    ret = db.insert(i_sql)
                pass
                self.logger.info(" 成功插入地市的月度指标区间(RAS_FT_SCOPE_CITY_MONTH)记录数 = :%s",count)
            pass
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",i_sql)
        finally:
            db.close()
        pass
    pass
    ######################################################################
    # 获取计算多元线性回归所需的数据源,并计算
    ######################################################################
    def proMultiRegData(self, data=None,  group_code=None, fts=None):

        # 1.1. 查询源数据
        # 获取评分数据
        s_sql = ""
        rec = None
        score_data = None
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            # 1.查询需要处理的规格
            s_sql = "select GROUP_CODE, CITY_CODE,CITY_NAME, BAR_CODE, BUSI_DATE, STD_FACTOR_SCORE SCORE " \
                    + " from RAS_SCORE_HEALTH_MONTH " \
                    "  WHERE GROUP_CODE = '" + group_code + "' and PRO_CODE='0' and STD_FACTOR_SCORE is not null " \
                    " order by  BAR_CODE, CITY_CODE, BUSI_DATE  "
            rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            score_data = pd.DataFrame(list(rec), columns=['GROUP_CODE', 'CITY_CODE', 'CITY_NAME',
                                                          'BAR_CODE',  'BUSI_DATE', 'SCORE'])
            # data.fillna(0.0, inplace=True)
            self.logger.info("评分数据记录数=:%s",len(score_data))

            #2 评分数据区间处理
            # 2.1 插入前先删除表
            d_score_sql = "delete from RAS_SCORE_HEALTH_SCOPE_COUNTRY_MONTH where GROUP_CODE='" + group_code + "' "
            ret = db.delete(d_score_sql)
            self.logger.info(" 市场健康度综合得分区间（全国）表删除成功= %s", str(ret))
            d_score_sql = "delete from RAS_SCORE_HEALTH_SCOPE_CITY_MONTH where GROUP_CODE='" + group_code + "' "
            ret = db.delete(d_score_sql)
            self.logger.info(" 市场健康度综合得分区间（分地市）表删除成功= %s", str(ret))
            count = 0
            # 全国
            PRE_SQL_COUNTRY = "INSERT INTO RAS_SCORE_HEALTH_SCOPE_COUNTRY_MONTH (GROUP_CODE, SCORE_MEAN, SCORE_STDEV," \
                              " SCORE_MAX, SCORE_MIN,  SCORE_DLINE, SCORE_ULINE ) VALUES "
            PRE_SQL_CITY = "INSERT INTO RAS_SCORE_HEALTH_SCOPE_CITY_MONTH (GROUP_CODE, CITY_CODE, CITY_NAME, " \
                           " SCORE_MEAN, SCORE_STDEV, SCORE_MAX, SCORE_MIN,  SCORE_DLINE, SCORE_ULINE ) VALUES "

            stat = self.getStatics(score_data.drop(score_data.columns[0:4], axis=1, inplace=False))
            max = stat.loc['max','SCORE']
            min = stat.loc['min','SCORE']
            mean = stat.loc['mean','SCORE']
            stdv = stat.loc['std','SCORE']
            dline = stat.loc['50%','SCORE']
            uline = stat.loc['75%','SCORE']
            i_country_sql = PRE_SQL_COUNTRY + " ('" + group_code + "', " \
                            + str(mean) + "," + str(stdv) + "," + str(max) + "," \
                            + str(min) + "," + str(dline) + "," + str(uline) + " )"
            ret = db.insert(i_country_sql)
            self.logger.info(" 成功插入市场健康度综合得分区间（全国）(RAS_SCORE_HEALTH_SCOPE_COUNTRY_MONTH)=%s",str(ret))

            # 地市
            i_city_sql = ""
            codes_df = score_data[['CITY_CODE','CITY_NAME']]
            city_codes = codes_df.drop_duplicates(subset=['CITY_CODE','CITY_NAME'], inplace=False)
            for row in city_codes.index:
                city_code = city_codes.loc[row,'CITY_CODE']
                city_name = city_codes.loc[row,'CITY_NAME']
                city_df = score_data[score_data['CITY_CODE']==city_code]
                city_stat = self.getStatics(city_df.drop(city_df.columns[0:4], axis=1, inplace=False))
                max = city_stat.loc['max','SCORE']
                min = city_stat.loc['min','SCORE']
                mean = city_stat.loc['mean','SCORE']
                stdv = city_stat.loc['std','SCORE']
                dline = city_stat.loc['50%','SCORE']
                uline = city_stat.loc['75%','SCORE']
                count = count + 1
                i_city_sql = i_city_sql + " ('" + group_code + "', '" + city_code + "','" + city_name + "', " \
                                + str(mean) + "," + str(stdv) + "," + str(max) + "," \
                                + str(min) + "," + str(dline) + "," + str(uline) + " ),"
            pass
            if (i_city_sql != ""):
                i_city_sql = i_city_sql.rstrip(',')
                i_city_sql = PRE_SQL_CITY + i_city_sql
                ret = db.insert(i_city_sql)
            pass
            self.logger.info(" 成功插入市场健康度综合得分区间（分地市）(RAS_SCORE_HEALTH_SCOPE_CITY_MONTH)记录数 = :%s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",s_sql)
        finally:
            db.close()
        pass
        if len(rec) <= 0:
            return None
        pass

        #1.3 数据预处理
        # self.cur_data.to_excel("/home/rasmode/tensorflow/shell/old_data.xlsx", index=False)
        data = self.preProcessData(group_code, data,'1')
        self.logger.info(" 数据预处理记录数=:%s",len(data))
        #1.3.1 计算指标区间范围(用于模拟仿真)
        self.countSimScope(group_code, data, fts)
        self.logger.info(" 计算指标区间范围(用于模拟仿真)完成 ")
        # 数据合并
        rst_df = pd.merge(score_data, data, how='inner', on=['CITY_CODE','CITY_NAME','BAR_CODE','BUSI_DATE'])
        self.logger.info(" 合并后的数据记录数=:%s",len(rst_df))
        self.logger.info(" 合并后的数据结构=:%s\n",rst_df.columns)

        # rst_df.to_excel("/home/rasmode/tensorflow/shell/all_data.xlsx", index=False)

        #1.4 获取最优变量
        forward_df = rst_df.drop(['GROUP_CODE', 'CITY_CODE', 'CITY_NAME', 'BAR_CODE', 'BAR_NAME',
                                  'BUSI_DATE'], axis=1, inplace=False)
        #1.4.1 全变量多元线性回归
        # all_cols = list(forward_df.columns)
        x_cols = list(forward_df.columns)
        x_cols.remove('SCORE')
        X_ALL = rst_df.loc[:,x_cols]
        Y = rst_df.loc[:,'SCORE']
        lr  = LinearRegression()
        lr.fit(X_ALL,Y)
        r2 = lr.score(X_ALL,Y)
        param = {}
        param['coefficient'] = lr.coef_
        param['intercept'] = lr.intercept_
        param['r2'] = r2
        param['formula'] = ''
        self.logger.info(" 原始的回归模型的参数:%s\n",param)
        # 保存回归结果
        cols = X_ALL.columns
        vif = self.countVIF(forward_df, 'SCORE')
        # print("vif = %s\n", vif)
        self.logger.info(" vif参数:%s\n",vif)
        self.saveLRParamToDB(group_code,cols,param,vif,pro_code='0')

        #1.4.2 逐步回归
        model, selected_ft_all  = self.forward_selected(forward_df,"SCORE")
        formula = model.model.formula
        params = model.params
        rsquared = model.rsquared_adj
        self.logger.info(" 最后的回归模型选择的变量:%s\n",selected_ft_all)
        self.logger.info(" 最后的回归模型:%s\n",formula)
        self.logger.info(" 最后的回归模型参数:%s\n",params)
        self.logger.info(" 最后的回归模型调整后的R2:%s",rsquared)
        selected_cols = "('" + "','".join(selected_ft_all) + "')"
        X = rst_df.loc[:,eval(selected_cols)]
        linreg  = LinearRegression()
        linreg.fit(X,Y)
        self.logger.info("优化后模型参数coefficients(b1,b2...):%s\n",linreg.coef_)
        self.logger.info("优化后模型参数intercept(b0):%s",linreg.intercept_)
        r2 = linreg.score(X,Y)
        param_all = {}
        param_all['coefficient'] = linreg.coef_
        param_all['intercept'] = linreg.intercept_
        param_all['r2'] = r2
        self.logger.info(" 优化后模型的R2:%s",r2)
        param_all['formula'] = formula
        # 保存回归结果
        cols = X.columns
        self.logger.info(" 最后的param参数:%s\n",param_all)
        all_cols = "('SCORE', '" + "','".join(selected_ft_all) + "')"
        vif = self.countVIF(rst_df.loc[:,eval(all_cols)], 'SCORE')
        # self.logger.info(" 最后的vif参数:%s\n",vif)
        self.saveLRParamToDB(group_code,cols,param_all,vif,pro_code='1')

        # 1.5 分地市处理:各地市的模型参数
        # 地市
        self.logger.info(" 开始利用逐步回归处理各地市地市模型参数")
        codes_df = rst_df[['CITY_CODE','CITY_NAME']]
        city_codes = codes_df.drop_duplicates(subset=['CITY_CODE','CITY_NAME'], inplace=False)
        db.connect()
        try:
            #1.5.1 插入前先删除调整指标值月表
            # d_sql = "delete from RAS_ADJUST_FT_MONTH where GROUP_CODE='" + group_code + "'"
            # db.delete(d_sql)
            #1.5.2 插入前先删除模型结果月表(分地市)
            d_sql = "delete from RAS_RESULT_MODEL_CITY_MONTH where GROUP_CODE='" + group_code + "'"
            db.delete(d_sql)
            # 分地市处理
            for row in city_codes.index:
                city_code = city_codes.loc[row,'CITY_CODE']
                city_name = city_codes.loc[row,'CITY_NAME']
                city_df = rst_df[rst_df['CITY_CODE']==city_code]
                forward_df = city_df.drop(['GROUP_CODE', 'CITY_CODE', 'CITY_NAME', 'BAR_CODE', 'BAR_NAME',
                                          'BUSI_DATE'], axis=1, inplace=False)

                param = None
                cols = None
                try:
                    #6.1.2 逐步回归
                    model, selected_ft  = self.forward_selected(forward_df,"SCORE")
                    # self.logger.info(" 优化后模型=:%s",city_code + city_name)
                    # self.logger.info(" 优化后模型选择的变量:%s\n",selected_ft)
                    selected_cols = "('" + "','".join(selected_ft) + "')"
                    X = forward_df.loc[:,eval(selected_cols)]
                    Y = forward_df.loc[:,'SCORE']
                    linreg  = LinearRegression()
                    linreg.fit(X,Y)
                    # self.logger.info("优化后模型参数coefficients(b1,b2...):%s\n",linreg.coef_)
                    # self.logger.info("优化后模型参数intercept(b0):%s",linreg.intercept_)
                    r2 = linreg.score(X,Y)
                    param = {}
                    param['coefficient'] = linreg.coef_
                    param['intercept'] = linreg.intercept_
                    param['r2'] = r2
                    # self.logger.info(" 优化后模型的R2:%s",r2)
                    # 保存回归结果
                    cols = X.columns
                except Exception as ex:
                    self.logger.info(" 地市=:%s",city_code + city_name)
                    self.logger.error(" 逐步回归错误:\n%s",str(ex))
                    # 如果出错,采用全国模型
                    self.saveLRParamToDBForCity(db,group_code,selected_ft_all,param_all,selected_ft_all,city_code,'1')
                else:
                    self.saveLRParamToDBForCity(db,group_code,cols,param,selected_ft_all,city_code,'1')
                pass
            pass
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
        finally:
            db.close()
        pass
        self.logger.info(" 各地市地市模型参数处理结束 ")
    pass
    ######################################################################
    # 计算指标区间范围(用于模拟仿真)
    ######################################################################
    def countSimScope(self, group_code='', data=None, fts=None):
        stat_sim = self.getStatics(data.drop(data.columns[0:5], axis=1, inplace=False))
        cols = stat_sim.columns
        self.logger.info("调整后的数据指标项:\n%s",cols)

        # 3.二级指标权重/得分计算
        db = MysqlDB(logger=self.logger)
        i_sql = ""
        try:
            db.connect()

            # 3.2.4 计算一级指标/二级指标区间
            self.logger.info(" 开始计算月度模拟仿真一级指标/二级指标区间...")
            # 1.0 查询反向指标
            reverse_ft = self.getReverseFeatures(group_code)

            # 二级指标--全国
            # 3.2.4.0 插入全国的月度指标区间表
            # 3.2.4.1 插入前先删除全国的月度指标区间表
            d_sql = "delete from RAS_FT_SCOPE_SIM_COUNTRY_MONTH where GROUP_CODE='" + group_code + "'"
            db.delete(d_sql)
            # 3.2.4.3 插入前先删除地市的月度指标区间
            d_sql = "delete from RAS_FT_SCOPE_SIM_CITY_MONTH where GROUP_CODE='" + group_code + "'"
            db.delete(d_sql)

            count = 0
            PRE_SQL_COUNTRY = "INSERT INTO RAS_FT_SCOPE_SIM_COUNTRY_MONTH (GROUP_CODE, FIRST_FT_CODE, " \
                              " SECOND_FT_CODE, SECOND_FT_MEAN, SECOND_FT_STDEV," \
                              " SECOND_FT_DLINE,SECOND_FT_ULINE, SECOND_FT_MAX, SECOND_FT_MIN) VALUES "
            PRE_SQL_CITY = "INSERT INTO RAS_FT_SCOPE_SIM_CITY_MONTH (GROUP_CODE, CITY_CODE, CITY_NAME, " \
                           " FIRST_FT_CODE,SECOND_FT_CODE, SECOND_FT_MEAN, SECOND_FT_STDEV, SECOND_FT_DLINE," \
                           " SECOND_FT_ULINE, SECOND_FT_MAX, SECOND_FT_MIN) VALUES "
            i_country_sql = ""
            for ft in fts:
                group_code = ft[0]
                first_code = ft[1]
                first_name = ft[2]
                second_code = ft[3]

                # 二级指标
                ft_dline = stat_sim.loc['50%',second_code]
                ft_uline = stat_sim.loc['75%',second_code]
                max = stat_sim.loc['max',second_code]
                min = stat_sim.loc['min',second_code]
                mean = stat_sim.loc['mean',second_code]
                stdv = stat_sim.loc['std',second_code]

                count = count + 1
                i_country_sql = i_country_sql + " ('" + group_code + "', '" +  first_code + "','" \
                                + second_code + "', " + str(mean)+ "," + str(stdv) + "," + str(ft_dline) + "," \
                                + str(ft_uline) + "," + str(max) + "," + str(min)  + "),"
            pass
            if (i_country_sql != ""):
                i_country_sql = i_country_sql.rstrip(',')
                i_country_sql = PRE_SQL_COUNTRY + i_country_sql
                ret = db.insert(i_country_sql)
            pass
            self.logger.info(" 成功插入全国的月度模拟指标区间表(RAS_FT_SCOPE_SIM_COUNTRY_MONTH)记录数 = :%s",count)

            # 二级指标--地市
            # 3.2.4.2 插入地市的月度指标区间
            code_df = data[['CITY_CODE', 'CITY_NAME']]
            city_codes = code_df.drop_duplicates(subset=['CITY_CODE','CITY_NAME'], inplace=False)
            count = 0
            i_sql = ""
            for row in city_codes.index:
                city_code = city_codes.loc[row,'CITY_CODE']
                city_name = city_codes.loc[row,'CITY_NAME']
                city_df = data[data['CITY_CODE']==city_code]
                city_stat = self.getStatics(city_df.drop(city_df.columns[0:5], axis=1, inplace=False))
                for ft in fts:
                    group_code = ft[0]
                    first_code = ft[1]
                    first_name = ft[2]
                    second_code = ft[3]
                    second_name = ft[4]
                    # 二级指标
                    ft_dline = city_stat.loc['50%',second_code]
                    ft_uline = city_stat.loc['75%',second_code]
                    max = city_stat.loc['max',second_code]
                    min = city_stat.loc['min',second_code]
                    mean = city_stat.loc['mean',second_code]
                    stdv = city_stat.loc['std',second_code]

                    count = count + 1
                    i_sql = i_sql + " ('" + group_code + "','" + city_code +"','" + city_name + "','" \
                            + first_code + "','" + second_code + "', " \
                            + str(mean) + "," + str(stdv)+ "," + str(ft_dline)+ ","  +  str(ft_uline) + "," \
                            + str(max) + "," + str(min)  + "),"
                    if count%2000 == 0:
                        i_sql = i_sql.rstrip(',')
                        i_sql = PRE_SQL_CITY + i_sql
                        ret = db.insert(i_sql)
                        i_sql = ""
                    pass
                pass
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL_CITY + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info(" 成功插入地市的月度模拟指标区间(RAS_FT_SCOPE_SIM_CITY_MONTH)记录数 = :%s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",i_sql)
        finally:
            db.close()
        pass
    pass

    ######################################################################
    # 保存多元线性回归计算的参数(分地市)
    ######################################################################
    def saveLRParamToDBForCity(self, db=None, group_code='', city_cols=None, param=None,
                               country_cols=None,city_code='', pro_code='0'):
        # 1.多元线性参数
        coef = param['coefficient']
        inter = param['intercept']
        r2 = param['r2']
        PRE_SQL = "INSERT INTO RAS_RESULT_MODEL_CITY_MONTH (GROUP_CODE, PRO_CODE, CITY_CODE, " \
                  " SECOND_FT_CODE, SECOND_FT_PARAM, INTERCEPT_PARAM, VIF_ORI) VALUES "
        i_a_sql = ""
        count = 0
        for col in city_cols:
            arg = coef[count]
            roi = r2
            if roi is None  or str(roi) == "inf" or str(roi) == "INF":
                roi = 'null'
            pass
            i_a_sql = i_a_sql + " ('" + group_code + "', '" + pro_code + "', '" + city_code + "', '" + col + "' " \
                      + ", " + str(arg) + "," + str(inter) + "," + str(roi) + "),"
            count += 1
        pass
        i_a_sql = i_a_sql.rstrip(',')
        i_a_sql = PRE_SQL + i_a_sql

        # # 2.保存可调整指标值
        # i_sql = "insert into RAS_ADJUST_FT_MONTH (GROUP_CODE, PRO_CODE, BUSI_DATE, CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, " \
        #         " FIRST_FT_CODE, FIRST_FT_NAME, FIRST_FT_SCORE,SECOND_FT_CODE,SECOND_FT_NAME, SECOND_FT_SCORE," \
        #         " SECOND_VALUE,SECOND_TRAN_VALUE, SECOND_STD_VALUE) " \
        #         " select a.GROUP_CODE, '1', a.BUSI_DATE, a.CITY_CODE, a.CITY_NAME, a.BAR_CODE, a.BAR_NAME, " \
        #         " a.FIRST_FT_CODE, b.FIRST_FT_NAME, a.FIRST_FT_SCORE, a.SECOND_FT_CODE,b.SECOND_FT_NAME, a.SECOND_FT_SCORE," \
        #         " a.SECOND_VALUE,a.SECOND_TRAN_VALUE,a.SECOND_STD_VALUE from RAS_FT_SCORE_MONTH a, RAS_FEATURE_CONFIG b " \
        #         " where a.GROUP_CODE='" + group_code + "' and a.CITY_CODE='" + city_code + "'" +\
        #         " and a.SECOND_FT_CODE in ('" + "','".join(country_cols) + "') and " + \
        #         " a.GROUP_CODE=b.GROUP_CODE and a.FIRST_FT_CODE=b.FIRST_FT_CODE and a.SECOND_FT_CODE = b.SECOND_FT_CODE " + \
        #         " and a.PRO_CODE = '0' "
        try:
            #2. 插入模型结果月表(分地市)
            ret = db.insert(i_a_sql)
            # self.logger.info(" 成功插入模型结果月表(分地市)(RAS_RESULT_MODEL_CITY_MONTH)记录数 = :%s",ret)
            # ret = db.insert(i_sql)
            # # self.logger.info(" 成功插入调整指标值月表(RAS_ADJUST_FT_MONTH)记录数 = :%s",ret)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",i_a_sql)
        pass
    pass

    ######################################################################
    # 保存多元线性回归计算的参数
    ######################################################################
    def saveLRParamToDB(self, group_code='', cols=None, param=None, vif=None, pro_code='0'):
        coef = param['coefficient']
        inter = param['intercept']
        r2 = param['r2']
        formula = param['formula']
        i_m_sql = "INSERT INTO RAS_ADJUST_MODEL_MONTH (GROUP_CODE, PRO_CODE, FORMULA, RSQUARED_ADJ) VALUES " \
                "('" + group_code + "','" + pro_code + "','" + str(formula) + "'," + str(r2) + ")"
        PRE_SQL = "INSERT INTO RAS_RESULT_MODEL_MONTH (GROUP_CODE, PRO_CODE, SECOND_FT_CODE, SECOND_FT_PARAM," \
                  " INTERCEPT_PARAM, VIF_ORI) VALUES "
        i_a_sql = ""
        count = 0
        for col in cols:
            arg = coef[count]
            roi = vif[vif['features']==col]['VIF'].values[0]
            if roi is None  or str(roi) == "inf" or str(roi) == "INF":
                roi = 'null'
            pass
            i_a_sql = i_a_sql + " ('" + group_code + "', '" + pro_code + "', '" + col + "' " \
                                + ", " + str(arg) + "," + str(inter) + "," + str(roi) + "),"
            count += 1
        pass
        i_a_sql = i_a_sql.rstrip(',')
        i_a_sql = PRE_SQL + i_a_sql
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            #1. 插入前先删除调整的模型参数月表
            d_sql = "delete from RAS_ADJUST_MODEL_MONTH " \
                    " where GROUP_CODE='" + group_code + "' and PRO_CODE='" + pro_code + "'"
            db.delete(d_sql)
            ret = db.insert(i_m_sql)
            self.logger.info(" 成功插入调整的模型参数月表(RAS_ADJUST_MODEL_MONTH)记录数 = :%s",ret)
            #2. 插入前先删除模型结果月表
            d_sql = "delete from RAS_RESULT_MODEL_MONTH " \
                    " where GROUP_CODE='" + group_code + "' and PRO_CODE='" + pro_code + "'"
            db.delete(d_sql)
            ret = db.insert(i_a_sql)
            self.logger.info(" 成功插入模型结果月表(RAS_RESULT_MODEL_MONTH)记录数 = :%s",ret)
            #3 插入前先删除调整指标值月表
            d_sql = "delete from RAS_ADJUST_FT_MONTH where GROUP_CODE='" + group_code + "'"
            db.delete(d_sql)
            # 3.1.保存可调整指标值
            i_sql = "insert into RAS_ADJUST_FT_MONTH (GROUP_CODE, PRO_CODE, BUSI_DATE, CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, " \
                    " FIRST_FT_CODE, FIRST_FT_NAME, FIRST_FT_SCORE,SECOND_FT_CODE,SECOND_FT_NAME, SECOND_FT_SCORE," \
                    " SECOND_VALUE,SECOND_TRAN_VALUE, SECOND_STD_VALUE) " \
                    " select a.GROUP_CODE, '1', a.BUSI_DATE, a.CITY_CODE, a.CITY_NAME, a.BAR_CODE, a.BAR_NAME, " \
                    " a.FIRST_FT_CODE, b.FIRST_FT_NAME, a.FIRST_FT_SCORE, a.SECOND_FT_CODE,b.SECOND_FT_NAME, a.SECOND_FT_SCORE," \
                    " a.SECOND_VALUE,a.SECOND_TRAN_VALUE,a.SECOND_STD_VALUE from RAS_FT_SCORE_MONTH a, RAS_FEATURE_CONFIG b " \
                    " where a.GROUP_CODE='" + group_code + "' " + \
                    " and a.SECOND_FT_CODE in ('" + "','".join(cols) + "') and " + \
                    " a.GROUP_CODE=b.GROUP_CODE and a.FIRST_FT_CODE=b.FIRST_FT_CODE and a.SECOND_FT_CODE = b.SECOND_FT_CODE " + \
                    " and a.PRO_CODE = '0' "
            ret = db.insert(i_sql)
            self.logger.info(" 成功插入调整指标值月表(RAS_ADJUST_FT_MONTH)记录数 = :%s",ret)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",i_a_sql)
        finally:
            db.close()
        pass
    pass
pass
if __name__ == "__main__":
    # 0.忽视警告，并屏蔽警告
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
    warnings.filterwarnings("ignore", category=Warning)

    argparser = argparse.ArgumentParser(description=" use type for process.")
    argparser.add_argument(
        '-a',
        '--action_type',
        help="action type, 0:否,1:定期加工,2:临时加工",
        default='1')
    args = argparser.parse_args()
    action_type = os.path.expanduser(args.action_type)
    factor = RASAnalyze(action_type=action_type)
    factor.main()
pass
