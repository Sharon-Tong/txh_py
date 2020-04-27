#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2017-08-31
#########################################################
import numpy as np
from com.ctitc.bigdata.db.mysqldb import MysqlDB
from com.ctitc.bigdata.model.ras.ras_base import RASBase
import pandas as pd
from com.ctitc.bigdata.model.ras.ras_analyze import RASAnalyze

import warnings
import argparse
import os

#########################################################
# 市场健康状态评价分析及模拟仿真分析模型（RAS）
# 市场健康度评价是通过一系列业务指标以得分的形式评价市场
# 在一段时间内的表现。
# 处理步骤:1)查询数据库获取记录,2)计算因子得分
#########################################################
class RASRevalue(RASBase):
    ##########################################
    # 因子分析模型初始化
    ##########################################
    def __init__(self, action_type='1'):
        super().__init__(logkey="ras_rev",action_type=action_type)
    pass

    ##########################################
    # 主处理函数
    ##########################################
    def main(self):
        self.logger.info("===========================START Revalue====================")
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

        cnt = 0
        for item in arr_d_1:
            task_msg = arr_d_3[cnt][0] + "(" + arr_d_4[cnt][0] + ")"
            self.logger.info("开始任务处理:%s",task_msg)
            self.logger.info("正在处理的规格:%s",",".join(arr_d_2[cnt]) + "(" + ",".join(item) + ")")
            # print("\n正在处理的规格%s" % (item))
            try:
                # corr = self.getStdData(item)
                corr = self.getStdData(arr_d_4[cnt], item)
                if corr is not None:
                    self.doPCA(corr, 0.8, arr_d_4[cnt], arr_d_3[cnt])
                pass
            except Exception as ex:
                self.logger.error(str(ex))
                self.logger.info("任务处理异常:%s",task_msg)
            pass
            cnt += 1
            self.logger.info("结束任务处理:%s",task_msg)
        pass
        self.logger.info("===========================END Revalue====================")
    pass
    ##########################################
    # 从数据库读取原始数据并标准化,返回相关系数矩阵
    ##########################################
    def getStdData(self, grp="", bars=None):
        group_code = grp[0]
        # 1.初始化数据库, 并查询数据
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

        # 2.从大数据平台获取数据源
        data = self.getCityInfoFromHive(sql_condition, '1')
        # rec, data = self.getCityInfoFromHive_AUTO(group_code, sql_condition, '1')
        self.cur_rec = data

        if len(self.cur_rec) <= 0:
            return None
        pass

        self.X = data.drop(data.columns[0:5], axis=1, inplace=False)
        # self.logger.info("原始数据:\n%s",self.X)
        # print("\n原始数据:\n", self.X)

        # 2. 0均值规范化
        self.X_std = (self.X - self.X.mean())/self.X.std()
        self.X_row = self.X_std.shape[0]
        self.X_col = self.X_std.shape[1]
        # 3.相关系数矩阵
        C = self.X_std.corr()
        # self.logger.info("标准值:\n%s",self.X_std)
        self.logger.info("相关系数矩阵:\n%s",C)
        return C
    pass
    #########################################
    # 因子分析
    ##########################################
    def doPCA(self, C, ratio, grp=None, grp_nm=None, pro_code='0'):
        # 0.获取系列规格
        grp_code = grp[0]
        grp_name = grp_nm[0]
        self.logger.info("grp_code:\n%s",grp_code)
        # print("\ngrp_code:\n", grp_code)
        pca_t_score, fa_t_score = self.getScore(self.X_std,C, ratio,grp_code, grp_name, 'RAS_LOADING_HEALTH_MONTH')

        # 4. 结果保存到数据库中
        db = MysqlDB(logger=self.logger)
        i_score_sql = ""
        try:
            db.connect()
            # 2.插入主成分综合得分表
            # 2.1 插入前先删除主成分综合得分表
            d_score_sql = "delete from RAS_SCORE_HEALTH_MONTH where GROUP_CODE='" + grp_code + "' " \
                             " and PRO_CODE='" + pro_code + "'"
            db.delete(d_score_sql)
            count = 0
            PRE_SQL = "INSERT INTO RAS_SCORE_HEALTH_MONTH (GROUP_CODE, BUSI_DATE, PRO_CODE, CITY_CODE, CITY_NAME, " \
                      " BAR_CODE, BAR_NAME,  PCA_SCORE, FACTOR_SCORE, STD_PCA_SCORE, STD_FACTOR_SCORE) VALUES "
            self.logger.info("self.cur_rec = %s",len(self.cur_rec))
            self.logger.info("fa_t_score = %s",len(fa_t_score))
            for row in self.cur_rec.index:
                bar_code = self.cur_rec.loc[row, 'BAR_CODE']
                bar_name = self.cur_rec.loc[row, 'BAR_NAME']
                if bar_name is None or str(bar_name) == "nan" or str(bar_name) == "NaN":
                    bar_name = "null"
                pass

                city_code = self.cur_rec.loc[row, 'CITY_CODE']
                city_name = self.cur_rec.loc[row, 'CITY_NAME']
                if city_name is None or str(city_name) == "nan" or str(city_name) == "NaN":
                    city_name = "null"
                pass
                busi_date = self.cur_rec.loc[row, 'BUSI_DATE']
                pca_score = pca_t_score[count, 0]
                std_pca_score = 0.0
                if pca_score is None or str(pca_score) == "nan" or str(pca_score) == "NaN":
                    pca_score = "null"
                    std_pca_score = "null"
                else:
                    # 标准分计算
                    std_pca_score = pca_score*10.0 + 60.0
                pass
                fat_score = fa_t_score[count]
                std_fat_score = 0.0
                if fat_score is None or str(fat_score) == "nan"  or str(fat_score) == "NaN":
                    fat_score = "null"
                    std_fat_score = "null"
                else:
                    # 标准分计算
                    std_fat_score = fat_score*10.0 + 60.0
                pass
                count = count + 1
                i_score_sql = i_score_sql + " ('" + grp_code + "', '" + busi_date + "', '" + pro_code + "', '" + city_code + "', " \
                              + " '" + city_name + "', '" + bar_code + "', '" + bar_name + "', "  + str(pca_score)+ "," \
                              + str(fat_score) + ", "  +  str(std_pca_score)+ "," + str(std_fat_score) + " ),"
                if count%1000 == 0:
                    i_score_sql = i_score_sql.rstrip(',')
                    i_score_sql = PRE_SQL + i_score_sql
                    ret = db.insert(i_score_sql)
                    i_score_sql = ""
                pass
            pass
            if (i_score_sql != ""):
                i_score_sql = i_score_sql.rstrip(',')
                i_score_sql = PRE_SQL + i_score_sql
                ret = db.insert(i_score_sql)
            pass
            self.logger.info(" 成功插入数据库记录数 = :%s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",i_score_sql)
        finally:
            db.close()
        pass
    pass
    ##########################################
    # 模拟主处理函数，用于更新数据，再计算市场状态得分
    # condition:数组，更新条件
    # item:字典，更新内容
    ##########################################
    def simulateScore(self, condition=None,fields=None):
        con_grp_code = condition[0]
        con_city_code = condition[1]
        con_busi_date = condition[2]
        con_bar_code = condition[3]
        # 1.初始化数据库, 并查询数据
        db = MysqlDB(logger=self.logger)
        barcodes = None
        try:
            db.connect()
            # 1.查询需要处理的规格及规格组别代码
            s_total = "select BAR_CODE,BAR_NAME, GROUP_NAME, GROUP_CODE from RAS_BAR_CONFIG " \
                      " where IS_USE = '" + action_type + "' and GROUP_CODE = '" + con_grp_code + "' order by  BAR_CODE "
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
        cnt = 0
        for item in arr_d_1:
            task_msg = arr_d_3[cnt][0] + "(" + arr_d_4[cnt][0] + ")"
            self.logger.info("开始任务处理:%s",task_msg)
            self.logger.info("正在处理的规格:%s",arr_d_2[cnt] + "(" + item + ")")
            # print("\n正在处理的规格%s" % (item))
            try:
                corr = self.updateStdData(item, condition, fields)
                if corr is not None:
                    self.doPCA(corr, 0.8, arr_d_4[cnt], arr_d_3[cnt], pro_code='1')
                pass
            except Exception as ex:
                self.logger.error(str(ex))
                self.logger.info("任务处理异常:%s",task_msg)
                # print(str(ex))
                # print("error")
            pass
            cnt += 1
            self.logger.info("结束任务处理:%s",task_msg)
        pass
    pass
    ##########################################
    # 从数据库读取原始数据,并修改
    ##########################################
    def updateStdData(self, bars=None, condition=None, fields=None):
        con_grp_code = condition[0]
        con_city_code = condition[1]
        con_busi_date = condition[2]
        con_bar_code = condition[3]
        # 1.初始化数据库, 并查询数据
        sql_condition = ""
        cnt = 0
        for key in bars:
            if sql_condition == "":
                sql_condition = " BAR_CODE = '" + key + "' "
            else:
                sql_condition += " OR BAR_CODE = '" + key + "' "
            pass
            cnt += 1
        pass

        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            # 1.查询需要处理的规格
            # 卷烟动销率=本周期卷烟实际销量÷（期初库存+本期购进）
            s_sql = "select CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, BUSI_DATE,  " \
                    " FACTOR_XL, FACTOR_XLZB, (FACTOR_XL/FACTOR_KCL) DXL, FACTOR_SCFE,  FACTOR_CGL, FACTOR_PHL," \
                    " FACTOR_HJXLB, FACTOR_LSHSB " \
                    " from RAS_FT_SRC_CITY_MONTH " \
                    "  WHERE FACTOR_XL > 0.0 and " + sql_condition  + " order by  BAR_CODE, CITY_CODE, BUSI_DATE  "
            self.cur_rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            data = pd.DataFrame(list(self.cur_rec), columns=['CITY_CODE', 'CITY_NAME', 'BAR_CODE', 'BAR_NAME',
                                                             'BUSI_DATE', 'FACTOR_XL', 'FACTOR_XLZB', 'DXL',
                                                             'FACTOR_SCFE', 'FACTOR_CGL', 'FACTOR_PHL',
                                                             'FACTOR_HJXLB', 'FACTOR_LSHSB'])
            data.fillna(0.0, inplace=True)
        finally:
            db.close()
        pass
        if len(self.cur_rec) <= 0:
            return None
        pass

        # 1.2 更新数据内容
        for key in fields.keys():
            value = fields[key]
            data[(data['CITY_CODE']==con_city_code & data['BUSI_DATE']==con_busi_date
                  & data['BAR_CODE']==con_bar_code)][key] = value
            if key == 'FACTOR_XL':
                old_value = data[(data['CITY_CODE']==con_city_code & data['BUSI_DATE']==con_busi_date
                                  & data['BAR_CODE']==con_bar_code)]['FACTOR_XL']
                tmp = data[(data['CITY_CODE']==con_city_code & data['BUSI_DATE']==con_busi_date
                            & data['BAR_CODE']==con_bar_code)]['DXL']
                if old_value > 0.0:
                    ratio = value * 1.00 / old_value
                    data[(data['CITY_CODE']==con_city_code & data['BUSI_DATE']==con_busi_date
                          & data['BAR_CODE']==con_bar_code)][key] = tmp * ratio
                pass
            pass
        pass

        self.X = data.drop(data.columns[0:6], axis=1, inplace=False)
        # self.logger.info("原始数据:\n%s",self.X)
        # print("\n原始数据:\n", self.X)

        # 2. 0均值规范化
        self.X_std = (self.X - self.X.mean())/self.X.std()
        self.X_row = self.X_std.shape[0]
        self.X_col = self.X_std.shape[1]
        # 3.相关系数矩阵
        C = self.X_std.corr()
        # self.logger.info("标准值:\n%s",self.X_std)
        self.logger.info("相关系数矩阵:\n%s",C)
        # print("\n标准值:\n", self.X_std)
        # print("\n相关系数矩阵:\n", C)
        return C
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

    # start revalue
    rModel = RASRevalue(action_type=action_type)
    rModel.main()
    # start analyze
    # aModel = RASAnalyze(action_type=action_type)
    # aModel.main()
pass
