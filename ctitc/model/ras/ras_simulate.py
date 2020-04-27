#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2019-08-24
#########################################################
import numpy as np
from com.ctitc.bigdata.db.mysqldb import MysqlDB
from com.ctitc.bigdata.model.ras.ras_base import RASBase
import pandas as pd

#########################################################
# 市场健康状态评价分析及模拟仿真分析模型（RAS）
# 模拟仿真是建立市场健康度得分和市场状态分析中涉及到的各指标细项
# 之间的数学模型，通过模型确定各指标细项的系数，进而模拟健康度得
# 分和指标细项值来预测新的得分和指标值。
#########################################################
class RASSimulate(RASBase):
    ##########################################
    # 因子分析模型初始化
    ##########################################
    def __init__(self):
        super().__init__(logkey="ras_sim")
    pass
    ##########################################
    # 主处理函数:1:调整指标
    ##########################################
    def mainFT(self, group_code, city_code, busi_date, bar_code, fields=None):
        self.logger.info("===========================Simulate Feature START====================")
        # 1.创建查询条件
        condition = []
        condition.append(group_code)
        condition.append(city_code)
        condition.append(busi_date)
        condition.append(bar_code)
        # 2.初始化数据库, 并查询数据
        db = MysqlDB(logger=self.logger)

        if fields is None:
            # 从数据库获取调整指标数据
            rows = ""
            s_sql = ""
            try:
                db.connect()
                # 1.查询调整指标值月表(RAS_ADJUST_FT_MONTH)
                s_sql = "SELECT GROUP_CODE, BUSI_DATE, CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, FIRST_FT_CODE, "  + \
                        " FIRST_FT_NAME, SECOND_FT_CODE, SECOND_FT_NAME,SECOND_UPD_VALUE FROM RAS_ADJUST_FT_MONTH "  + \
                        " WHERE GROUP_CODE='" + group_code + "' and CITY_CODE='" + city_code + "' and "  + \
                        " PRO_CODE='1' and BUSI_DATE='" + busi_date + "' and BAR_CODE='" + bar_code + "' and " + \
                        " SECOND_UPD_VALUE is not null "
                rows = db.select(s_sql)
                self.logger.info(" sql = :\n%s",s_sql)
            except Exception as ex:
                self.logger.error(" 数据库错误:\n%s",str(ex))
                self.logger.error(" sql = :\n%s",s_sql)
            finally:
                db.close()
            pass
            if len(rows) <= 0 :
                self.logger.error("record num is 0! exit system.")
                exit("record num is 0! exit system.")
            pass
            fields = {}
            for row in rows:
                key = row[8]
                value = row[10]
                fields[key] = value
            pass
        else:
            count = 0
            u_sql = ""
            try:
                db.connect()
                # 保存调整指标数据到数据库调整指标值月表(RAS_ADJUST_FT_MONTH)

                for col in fields.keys():
                    second_code = col
                    second_value = fields[col]
                    u_sql = "update RAS_ADJUST_FT_MONTH set SECOND_UPD_VALUE = " + str(second_value) + " where " +\
                           " GROUP_CODE='" + group_code + "' and PRO_CODE='1' and CITY_CODE='" + city_code + "' and " +\
                           " BUSI_DATE='" + busi_date + "' and SECOND_FT_CODE ='" + second_code + "' "
                    ret = db.update(u_sql)
                    count += 1
                pass
                self.logger.info(" 成功更新调整指标值月表(RAS_ADJUST_FT_MONTH)记录数 = :%s",count)
            except Exception as ex:
                self.logger.error(" 数据库错误:\n%s",str(ex))
                self.logger.error(" sql = :\n%s",u_sql)
            finally:
                db.close()
            pass
        pass
        #3.开始模拟调整指标,计算评分
        json_str = self.simpleSimulateFeature(condition, fields, pro_type='1')
        self.logger.info(" json_str = :\n%s",json_str)
        self.logger.info("===========================Simulate Feature END====================")
        return json_str
    pass
    ##########################################
    # 主处理函数:2:调整评分
    ##########################################
    def mainScore(self, group_code, city_code, busi_date, bar_code, score=None):
        self.logger.info("===========================Simulate Score START====================")
        # 1.创建查询条件
        condition = []
        condition.append(group_code)
        condition.append(city_code)
        condition.append(busi_date)
        condition.append(bar_code)
        # 2.初始化数据库, 并查询数据
        reverse = self.getReverseFeatures(group_code)
        db = MysqlDB(logger=self.logger)
        param_df = None
        s_sql = ""
        interc = 0.0
        try:
            db.connect()
            # 2.1.查询调整得分月表(RAS_ADJUST_SCORE_MONTH)
            if score is None or score == "":
                s_sql = "SELECT GROUP_CODE, BUSI_DATE, CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, UPD_FACTOR_SCORE, " \
                        " STD_FACTOR_SCORE FROM RAS_ADJUST_SCORE_MONTH " \
                        " WHERE GROUP_CODE='" + group_code + "' and CITY_CODE='" + city_code + "' and " \
                        + " BUSI_DATE='" + busi_date + "' and BAR_CODE='" + bar_code + "'"
                rows = db.select(s_sql)
                if len(rows) <= 0 :
                    self.logger.error("score is None! exit system.")
                    exit("score is None! exit system.")
                pass
                score = rows[0][6]
            else: # 保存得分
                d_sql = " delete from RAS_ADJUST_SCORE_MONTH " + \
                        " WHERE GROUP_CODE='" + group_code + "' and CITY_CODE='" + city_code + "' and " +\
                        " BUSI_DATE='" + busi_date + "' and BAR_CODE='" + bar_code + "'"
                db.delete(d_sql)
                i_sql = "insert into RAS_ADJUST_SCORE_MONTH (GROUP_CODE, BUSI_DATE, CITY_CODE, CITY_NAME, " \
                        " BAR_CODE, BAR_NAME, UPD_FACTOR_SCORE, STD_FACTOR_SCORE ) " + \
                        " select GROUP_CODE, BUSI_DATE, CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, " + str(score) + ","+ \
                        " STD_FACTOR_SCORE from RAS_SCORE_HEALTH_MONTH  " + \
                        " WHERE GROUP_CODE='" + group_code + "' and CITY_CODE='" + city_code + "' and " + \
                        " BUSI_DATE='" + busi_date + "' and PRO_CODE='0' and BAR_CODE='" + bar_code + "'"
                ret = db.insert(i_sql)
                self.logger.info(" 成功insert调整得分月表(RAS_ADJUST_SCORE_MONTH)记录数 = :%s",ret)
            pass
            #2.2 获取公式系数
            # s_sql = "SELECT a.GROUP_CODE, a.SECOND_FT_CODE, a.SECOND_FT_PARAM, a.INTERCEPT_PARAM, b.SECOND_FT_MAX, " \
            #         " b.SECOND_FT_MIN, c.SECOND_TRAN_VALUE FROM RAS_RESULT_MODEL_CITY_MONTH a,  RAS_FT_SCOPE_CITY_MONTH b," + \
            #         " RAS_ADJUST_FT_MONTH c " \
            #         " WHERE a.GROUP_CODE='" + group_code + "' and a.PRO_CODE='1' and a.GROUP_CODE = b.GROUP_CODE and " + \
            #         " a.GROUP_CODE = c.GROUP_CODE and c.PRO_CODE='1' and a.SECOND_FT_CODE=b.SECOND_FT_CODE " + \
            #         " and a.SECOND_FT_CODE=c.SECOND_FT_CODE and a.CITY_CODE=b.CITY_CODE and a.CITY_CODE=c.CITY_CODE " + \
            #         " and a.CITY_CODE='" + city_code + "' and c.BAR_CODE='" + bar_code + "'" + \
            #         " and c.BUSI_DATE='" + busi_date + "'"
            s_sql = "SELECT a.GROUP_CODE, a.SECOND_FT_CODE, a.SECOND_FT_PARAM, a.INTERCEPT_PARAM, b.SECOND_FT_MAX, " \
                    " b.SECOND_FT_MIN, c.SECOND_VALUE FROM RAS_RESULT_MODEL_MONTH a,  RAS_FT_SCOPE_SIM_CITY_MONTH b," + \
                    " RAS_ADJUST_FT_MONTH c " \
                    " WHERE a.GROUP_CODE='" + group_code + "' and a.PRO_CODE='1' and a.GROUP_CODE = b.GROUP_CODE and " + \
                    " a.GROUP_CODE = c.GROUP_CODE and c.PRO_CODE='1' and a.SECOND_FT_CODE=b.SECOND_FT_CODE " + \
                    " and a.SECOND_FT_CODE=c.SECOND_FT_CODE and c.CITY_CODE=b.CITY_CODE  " + \
                    " and c.CITY_CODE='" + city_code + "' and c.BAR_CODE='" + bar_code + "'" + \
                    " and c.BUSI_DATE='" + busi_date + "'"
            rec = db.select(s_sql)
            if len(rec) <= 0 :
                # 适用新品,刚上市
                s_sql = "SELECT a.GROUP_CODE, a.SECOND_FT_CODE, a.SECOND_FT_PARAM, a.INTERCEPT_PARAM, " +\
                            " b.SECOND_FT_MAX, b.SECOND_FT_MIN, c.SECOND_VALUE " +\
                        " FROM RAS_RESULT_MODEL_MONTH a " +\
                        " left join  RAS_FT_SCOPE_SIM_CITY_MONTH b " +\
                        " on a.GROUP_CODE = b.GROUP_CODE and a.SECOND_FT_CODE=b.SECOND_FT_CODE" + \
                        " left join (select group_code, SECOND_FT_CODE,SECOND_VALUE from RAS_ADJUST_FT_MONTH " +\
                        " where CITY_CODE='" + city_code + "' and PRO_CODE='1' and BAR_CODE='" + bar_code + "'" +\
                        " and BUSI_DATE='" + busi_date + "')  c" +\
                        " on a.GROUP_CODE = c.GROUP_CODE  and a.SECOND_FT_CODE=c.SECOND_FT_CODE " +\
                        " WHERE a.GROUP_CODE='" + group_code + "' and a.PRO_CODE='1' " +\
                        " and b.CITY_CODE='" + city_code + "'"
                rec = db.select(s_sql)
            pass
            self.logger.info(" SQL=:\n%s",s_sql)

            param_df = pd.DataFrame(list(rec), columns=['GROUP_CODE', 'SECOND_FT_CODE', 'arg',
                                                     'INTERCEPT_PARAM', 'max','min','b0'])
            for row in param_df.index:
                b = param_df.loc[row, 'b0']
                s_code = param_df.loc[row, 'SECOND_FT_CODE']
                if (str(b) == 'NaN' or str(b) == 'nan'):
                    param_df.loc[row, 'b0'] = param_df.loc[row, 'min']
                else:
                    if b != 0.0:
                        if s_code in reverse:
                            param_df.loc[row, 'b0'] = 1.0/b
                        pass
                    pass
                pass
            pass
            param_df.fillna(0.0, inplace=True)
            interc = param_df.iloc[0,3]
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",s_sql)
        finally:
            db.close()
        pass
        #3. 规划求解, 计算指标值
        x = self.getNLP(param_df, score, interc)
        self.logger.info(" 规划求解, 计算指标值=:\n%s",x)
        fields = {}
        old_fields = {}
        second_code = param_df['SECOND_FT_CODE'].values
        count = 0
        for item in x:
            s_code = second_code[count]
            if item != 0.0:
                if s_code in reverse:
                    fields[s_code] = 1.0 / item
                else:
                    fields[s_code] = item
                pass
            else:
                fields[s_code] = item
            pass
            old_fields[s_code] = item
            count += 1
        pass
        self.logger.info(" 规划求解, 计算指标原始值=:\n%s",old_fields)
        self.logger.info(" 规划求解, 计算指标转化值=:\n%s",fields)

        # 4.保存规划求解的结果
        try:
            db.connect()
            d_sql = " delete from RAS_ADJUST_FT_MONTH " + \
                    " WHERE GROUP_CODE='" + group_code + "' and CITY_CODE='" + city_code + "' and " + \
                    " BUSI_DATE='" + busi_date + "' and BAR_CODE='" + bar_code + "' and PRO_CODE='2' "
            db.delete(d_sql)
            i_a_sql = ""
            PRE_SQL = " insert into RAS_ADJUST_FT_MONTH (GROUP_CODE, PRO_CODE, BUSI_DATE, CITY_CODE, CITY_NAME, " \
                    " BAR_CODE, BAR_NAME, FIRST_FT_CODE, FIRST_FT_NAME, FIRST_FT_SCORE,SECOND_FT_CODE,SECOND_FT_NAME," \
                    " SECOND_FT_SCORE, SECOND_VALUE,SECOND_TRAN_VALUE, SECOND_UPD_VALUE, SECOND_STD_VALUE) "
            for col in fields.keys():
                t_value = fields[col]
                trans_value = old_fields[col]
                i_a_sql = " select a.GROUP_CODE, '2', a.BUSI_DATE, a.CITY_CODE, a.CITY_NAME, a.BAR_CODE, a.BAR_NAME, " +\
                         " a.FIRST_FT_CODE, b.FIRST_FT_NAME, a.FIRST_FT_SCORE, a.SECOND_FT_CODE, b.SECOND_FT_NAME, " + \
                         " a.SECOND_FT_SCORE,a.SECOND_VALUE," + str(trans_value) +  "," + str(t_value) + ", " +\
                         " a.SECOND_STD_VALUE from RAS_FT_SCORE_MONTH a, RAS_FEATURE_CONFIG b " + \
                         " WHERE a.GROUP_CODE=b.GROUP_CODE and a.FIRST_FT_CODE=b.FIRST_FT_CODE and " +\
                         " a.SECOND_FT_CODE = b.SECOND_FT_CODE and a.GROUP_CODE='" + group_code + "' and a.CITY_CODE='" +\
                         city_code + "' and a.BUSI_DATE='" + busi_date + "' and a.BAR_CODE='" + bar_code + "' and " +\
                         " a.PRO_CODE='0' and a.SECOND_FT_CODE='" + col + "' "
                count += 1
                i_a_sql = PRE_SQL + i_a_sql
                ret = db.insert(i_a_sql)
            pass
            self.logger.info(" 成功insert调整指标值月表(RAS_ADJUST_FT_MONTH)记录数 = :%s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",s_sql)
        finally:
            db.close()
        pass

        #4.开始模拟调整指标,计算评分
        json_str = self.simpleSimulateFeature(condition, fields, pro_type='2')
        self.logger.info(" json_str = :\n%s",json_str)
        self.logger.info("===========================Simulate Score END====================")
        return json_str
    pass
    ##########################################
    # 模拟主处理函数，用于快捷更新数据，再计算结果
    # 用途:调整指标,计算评分
    # condition:数组，更新条件
    # fields:字典，更新内容
    # pro_type:1:调整指标, 2:调整评分
    ##########################################
    def simpleSimulateFeature(self, condition=None, fields=None, pro_type='1'):
        #1. 条件参数
        group_code = condition[0]
        city_code = condition[1]
        busi_date = condition[2]
        bar_code = condition[3]
        #2. 查询相关得分数据
        # 1)全部指标得分
        s_ft_sql = "SELECT a.GROUP_CODE, a.BUSI_DATE, a.CITY_CODE, a.CITY_NAME, a.BAR_CODE, a.BAR_NAME, " \
                " a.FIRST_FT_CODE, a.FIRST_FT_SCORE, a.SECOND_FT_CODE, a.SECOND_FT_SCORE, " \
                " a.SECOND_VALUE, a.SECOND_STD_VALUE, a.SECOND_TRAN_VALUE, b.SECOND_UPD_VALUE " +\
                " FROM RAS_FT_SCORE_MONTH a left join  RAS_ADJUST_FT_MONTH b " + \
                " on a.GROUP_CODE=b.GROUP_CODE and a.CITY_CODE=b.CITY_CODE and a.BAR_CODE=b.BAR_CODE " +\
                " and a.BUSI_DATE=b.BUSI_DATE and a.SECOND_FT_CODE=b.SECOND_FT_CODE and b.PRO_CODE='" + pro_type + "' " +\
                " WHERE  a.PRO_CODE='0' and a.GROUP_CODE='" + group_code + "' and a.CITY_CODE=" \
                "'" + city_code + "' and  a.BUSI_DATE='" + busi_date + "' and a.BAR_CODE='" + bar_code + "' "
        # 2) 指标范围
        s_scope_sql = "SELECT a.GROUP_CODE, a.FIRST_FT_CODE, a.SECOND_FT_CODE, " \
                " a.FIRST_FT_MAX, a.FIRST_FT_MIN, a.FIRST_FT_MEAN, a.FIRST_FT_STDEV, " +\
                " a.SECOND_FT_MAX, a.SECOND_FT_MIN, a.SECOND_FT_MEAN, a.SECOND_FT_STDEV, b.CV_SECOND_WEIGHT " \
                " FROM RAS_FT_SCOPE_COUNTRY_MONTH a, RAS_FT_WEIGHT_MONTH b " \
                " WHERE a.GROUP_CODE=b.GROUP_CODE and a.SECOND_FT_CODE=b.SECOND_FT_CODE and " \
                " a.GROUP_CODE='" + group_code + "'"
        db = MysqlDB(logger=self.logger)
        # ft_data = None
        reverse = self.getReverseFeatures(group_code)
        json_str = ""
        try:
            db.connect()
            count = 0

            # 查询指标数据
            sim_rec = db.select(s_ft_sql)
            ft_data = pd.DataFrame(list(sim_rec), columns=['GROUP_CODE', 'BUSI_DATE', 'CITY_CODE', 'CITY_NAME',
                                                        'BAR_CODE', 'BAR_NAME','FIRST_FT_CODE', 'FIRST_FT_SCORE',
                                                        'SECOND_FT_CODE','SECOND_FT_SCORE', 'SECOND_VALUE',
                                                        'SECOND_STD_VALUE','SECOND_TRAN_VALUE','SECOND_UPD_VALUE'])

            # 预处理
            ft_data = self.preProcessData(group_code, ft_data)
            ft_data.fillna('null', inplace=True)

            scope_rec = db.select(s_scope_sql)
            scope_data = pd.DataFrame(list(scope_rec), columns=['GROUP_CODE', 'FIRST_FT_CODE',
                                                                'SECOND_FT_CODE','FIRST_FT_MAX', 'FIRST_FT_MIN',
                                                                'FIRST_FT_MEAN', 'FIRST_FT_STDEV','SECOND_FT_MAX',
                                                                'SECOND_FT_MIN','SECOND_FT_MEAN','SECOND_FT_STDEV',
                                                                'CV_SECOND_WEIGHT'])
            # scope_data.fillna(0.0, inplace=True)
            # 3.得分计算
            # 3.0 插入指标得分表
            # 3.1 插入前先删除指标得分表
            d_sql = "delete from RAS_FT_SCORE_MONTH where GROUP_CODE='" + group_code + "' and PRO_CODE='" + pro_type + "' " \
                   "and CITY_CODE='" + city_code + "' and BUSI_DATE='" + busi_date + "' and BAR_CODE='" + bar_code + "'"
            db.delete(d_sql)
            count = 0
            i_sql = ""
            PRE_SQL = "INSERT INTO RAS_FT_SCORE_MONTH (GROUP_CODE, BUSI_DATE, PRO_CODE, CITY_CODE, CITY_NAME, " \
                      " BAR_CODE, BAR_NAME, FIRST_FT_CODE, FIRST_FT_SCORE, SECOND_FT_CODE, SECOND_FT_SCORE, " \
                      " SECOND_VALUE, SECOND_TRAN_VALUE, SECOND_STD_VALUE) VALUES "
            code_df = ft_data[['FIRST_FT_CODE','SECOND_FT_CODE']]

            for row in ft_data.index:
                group_code = ft_data.loc[row]['GROUP_CODE']
                busi_date = ft_data.loc[row]['BUSI_DATE']
                city_code = ft_data.loc[row]['CITY_CODE']
                city_name = ft_data.loc[row]['CITY_NAME']
                bar_code = ft_data.loc[row]['BAR_CODE']
                bar_name = ft_data.loc[row]['BAR_NAME']
                first_code = ft_data.loc[row]['FIRST_FT_CODE']
                second_code = ft_data.loc[row]['SECOND_FT_CODE']
                t_value = ft_data.loc[row]['SECOND_VALUE']
                std_value = ft_data.loc[row]['SECOND_STD_VALUE']
                trans_value = ft_data.loc[row]['SECOND_TRAN_VALUE']
                upd_value = ft_data.loc[row]['SECOND_UPD_VALUE']
                second_score = ft_data.loc[row]['SECOND_FT_SCORE']
                first_score = ft_data.loc[row]['FIRST_FT_SCORE']

                sub_data = scope_data[(scope_data['GROUP_CODE'] == group_code) &
                                       (scope_data['SECOND_FT_CODE'] == second_code)]
                first_max = sub_data['FIRST_FT_MAX'].values[0]
                first_min = sub_data['FIRST_FT_MIN'].values[0]
                first_mean = sub_data['FIRST_FT_MEAN'].values[0]
                second_max = sub_data['SECOND_FT_MAX'].values[0]
                second_min = sub_data['SECOND_FT_MIN'].values[0]
                second_mean = sub_data['SECOND_FT_MEAN'].values[0]

                # 一级指标计算
                second_arr = code_df[code_df['FIRST_FT_CODE'] == first_code]['SECOND_FT_CODE'].values
                first_score = 0.0
                for s_col in second_arr:
                    sub_ft_data = ft_data[(ft_data['GROUP_CODE'] == group_code) &
                                      (ft_data['BUSI_DATE'] == busi_date) &
                                      (ft_data['CITY_CODE'] == city_code) &
                                      (ft_data['BAR_CODE'] == bar_code) &
                                      (ft_data['SECOND_FT_CODE'] == s_col)]
                    f_upd_value = sub_ft_data['SECOND_UPD_VALUE'].values[0]
                    f_std_value = sub_ft_data['SECOND_STD_VALUE'].values[0]
                    f_trans_value = sub_ft_data['SECOND_TRAN_VALUE'].values[0]
                    f_t_value = sub_ft_data['SECOND_VALUE'].values[0]
                    f_score = sub_ft_data['SECOND_FT_SCORE'].values[0]

                    sub_scope_data = scope_data[(scope_data['GROUP_CODE'] == group_code) &
                                          (scope_data['SECOND_FT_CODE'] == s_col)]
                    f_weight = sub_scope_data['CV_SECOND_WEIGHT'].values[0]
                    f_mean = sub_scope_data['SECOND_FT_MEAN'].values[0]
                    f_std = sub_scope_data['SECOND_FT_STDEV'].values[0]
                    f_max = sub_scope_data['SECOND_FT_MAX'].values[0]
                    f_min = sub_scope_data['SECOND_FT_MIN'].values[0]

                    if f_upd_value != str("null") :
                        if (f_max - f_min) != 0.0 :
                            if f_upd_value > f_max:
                                f_upd_value = f_max
                            elif f_upd_value < f_min:
                                f_upd_value = f_min
                            pass
                            s_mean = (f_upd_value - f_mean) /(f_max - f_min)
                            first_score = first_score + (s_mean * 100.0 + 60.0) * f_weight
                        pass
                    else:
                        if f_score != str("null") :
                            first_score = first_score + f_score * f_weight
                        pass
                    pass
                pass

                # 二级指标计算
                if upd_value != str("null"):
                    t_value = upd_value
                    if upd_value != 0  and (second_code in reverse):
                        t_value = 1.0 / upd_value
                    pass
                    if (second_max - second_min) != 0.0 :
                        if upd_value > second_max:
                            upd_value = second_max
                        elif upd_value < second_min:
                            upd_value = second_min
                        pass
                        std_value = (upd_value - second_min) / (second_max - second_min)
                        s_mean = (second_mean - second_min) / (second_max - second_min)
                        second_score = (std_value - s_mean) * 100.0 + 60.0
                        trans_value = upd_value
                    pass
                pass
                i_sql = i_sql + " ('" + group_code + "', '" +  busi_date + "', '" + pro_type + "', '" + city_code + "', " \
                         " '" + city_name + "', '" + bar_code + "', '" + bar_name + "', '"  + first_code + "'," \
                        + str(first_score) + ",'" + second_code + "',"  + str(second_score) + ", "  + str(t_value) + ", " \
                        +  str(trans_value) + "," + str(std_value) + " ),"
                count = count + 1
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info(" 成功插入指标得分月表(RAS_FT_SCORE_MONTH)记录数 = :%s",count)


            # 2.3 计算市场健康度得分
            # 2.3.1 查询模型结果,重载模型
            # s_sql = "SELECT GROUP_CODE, SECOND_FT_CODE, SECOND_FT_PARAM, INTERCEPT_PARAM FROM RAS_RESULT_MODEL_CITY_MONTH " \
            #         " WHERE GROUP_CODE='" + group_code + "' and PRO_CODE='1' and CITY_CODE='" + city_code + "'"
            # model_rec = db.select(s_sql)
            # if len(model_rec) <= 0 :
            #     s_sql = "SELECT GROUP_CODE, SECOND_FT_CODE, SECOND_FT_PARAM, INTERCEPT_PARAM FROM RAS_RESULT_MODEL_MONTH " \
            #             " WHERE GROUP_CODE='" + group_code + "' and PRO_CODE='1'"
            #     model_rec = db.select(s_sql)
            # pass
            s_sql = "SELECT GROUP_CODE, SECOND_FT_CODE, SECOND_FT_PARAM, INTERCEPT_PARAM FROM RAS_RESULT_MODEL_MONTH " \
                    " WHERE GROUP_CODE='" + group_code + "' and PRO_CODE='1'"
            model_rec = db.select(s_sql)

            model_data = pd.DataFrame(list(model_rec), columns=['GROUP_CODE', 'SECOND_FT_CODE','SECOND_FT_PARAM',
                                                                'INTERCEPT_PARAM'])
            model_data.fillna(0.0, inplace=True)

            s_sql = "SELECT GROUP_CODE, BUSI_DATE, CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, " \
                    " FIRST_FT_CODE, FIRST_FT_SCORE, SECOND_FT_CODE, SECOND_FT_SCORE, " +\
                    " SECOND_VALUE, SECOND_TRAN_VALUE, SECOND_STD_VALUE FROM  RAS_FT_SCORE_MONTH " + \
                    " where GROUP_CODE='" + group_code + "' and PRO_CODE='" + pro_type + "' " + \
                    " and CITY_CODE='" + city_code + "' and BUSI_DATE='" + busi_date + "' " + \
                     " and BAR_CODE='" + bar_code + "'"
            trans_rec = db.select(s_sql)
            trans_data = pd.DataFrame(list(trans_rec), columns=['GROUP_CODE', 'BUSI_DATE', 'CITY_CODE', 'CITY_NAME',
                                                                'BAR_CODE','BAR_NAME','FIRST_FT_CODE','FIRST_FT_SCORE',
                                                                'SECOND_FT_CODE','SECOND_FT_SCORE','SECOND_VALUE',
                                                                'SECOND_TRAN_VALUE', 'SECOND_STD_VALUE'])
            trans_data.fillna(0.0, inplace=True)
            score = 0.0
            city_name =""
            bar_name =""
            intercept = 0.0
            for row in model_data.index:
                second_code = model_data.loc[row]['SECOND_FT_CODE']
                param = model_data.loc[row]['SECOND_FT_PARAM']
                intercept = model_data.loc[row]['INTERCEPT_PARAM']
                t_value = trans_data.loc[trans_data.SECOND_FT_CODE == second_code, 'SECOND_VALUE'].values[0]
                city_name = trans_data.loc[trans_data.SECOND_FT_CODE == second_code, 'CITY_NAME'].values[0]
                bar_name = trans_data.loc[trans_data.SECOND_FT_CODE == second_code, 'BAR_NAME'].values[0]
                if t_value != 0  and (second_code in reverse):
                    t_value = 1.0 / t_value
                pass
                score = score + param * t_value
            pass
            score += intercept
            self.logger.info(" 调整指标后,市场状态得分 = :%s",score)
            # 2.3.2保存模拟得分
            i_sql = ""
            # 插入前先删除主成分综合得分表
            d_score_sql = "delete from RAS_SCORE_HEALTH_MONTH where GROUP_CODE='" + group_code + "' " \
                         " and PRO_CODE='" + pro_type + "' and CITY_CODE='" + city_code + "' and " \
                          " BUSI_DATE='" + busi_date + "' and BAR_CODE='" + bar_code + "'"
            db.delete(d_score_sql)
            i_sql = "INSERT INTO RAS_SCORE_HEALTH_MONTH (GROUP_CODE, BUSI_DATE, PRO_CODE, CITY_CODE, CITY_NAME, " \
                    " BAR_CODE, BAR_NAME, PCA_SCORE, FACTOR_SCORE, STD_PCA_SCORE, STD_FACTOR_SCORE) VALUES ( " \
                    " '" + group_code + "', '" + busi_date + "', '" + pro_type + "', '" + city_code + "', '" \
                    + city_name + "','" + bar_code + "', '" + bar_name + "', null, null, null," + str(score)  + " )"
            ret = db.insert(i_sql)
            self.logger.info(" 成功插入市场健康度综合得分月表(RAS_SCORE_HEALTH_MONTH) = :%s",ret)

            # 4.获取返回结果的json字符串
            s_sql = "SELECT a.GROUP_CODE, a.BUSI_DATE, a.CITY_CODE, a.CITY_NAME, a.BAR_CODE, a.BAR_NAME, " \
                    " a.FIRST_FT_CODE, c.FIRST_FT_NAME, a.FIRST_FT_SCORE, b.FIRST_FT_DLINE, b.FIRST_FT_ULINE, " + \
                    " b.FIRST_FT_MEAN, a.SECOND_FT_CODE, c.SECOND_FT_NAME, a.SECOND_FT_SCORE, a.SECOND_VALUE,  " + \
                    " b.SECOND_FT_SCORE_DLINE, b.SECOND_FT_SCORE_ULINE, b.SECOND_FT_SCORE_MEAN,  " + \
                    " b.SECOND_FT_DLINE_VIEW, b.SECOND_FT_ULINE_VIEW, b.SECOND_FT_MEAN_VIEW  " + \
                    " FROM  RAS_FT_SCORE_MONTH a, RAS_FT_SCOPE_COUNTRY_MONTH b, RAS_FEATURE_CONFIG c " + \
                    " where a.GROUP_CODE='" + group_code + "' and a.PRO_CODE='" + pro_type + "' " + \
                    " and a.CITY_CODE='" + city_code + "' and a.BUSI_DATE='" + busi_date + "' " + \
                    " and a.BAR_CODE='" + bar_code + "' and a.GROUP_CODE=b.GROUP_CODE and a.GROUP_CODE=c.GROUP_CODE " + \
                    " and a.FIRST_FT_CODE=b.FIRST_FT_CODE and a.SECOND_FT_CODE=b.SECOND_FT_CODE " + \
                    " and a.FIRST_FT_CODE=c.FIRST_FT_CODE and a.SECOND_FT_CODE=c.SECOND_FT_CODE " + \
                    " order by a.FIRST_FT_CODE, a.SECOND_FT_CODE "
            self.logger.info(" s_sql= :%s",s_sql)
            json_rec = db.select(s_sql)
            # 查询特征指标排序表
            s_sql = "SELECT FT_CODE, FT_NAME, FT_CODE_P, LEVEL, SORT, FT_POSTFIX " \
                    " FROM  RAS_CONFIG " + \
                    " order by FT_CODE_P, level, sort "
            sort_rec = db.select(s_sql)
            sort_df = pd.DataFrame(list(sort_rec), columns=['FT_CODE', 'FT_NAME', 'FT_CODE_P', 'LEVEL', 'SORT','FT_POSTFIX'])
            sort_df.fillna('', inplace=True)
            json_str = "{\"GROUP_CODE\":\"" + group_code + "\", \"BUSI_DATE\":\"" + busi_date + "\", \"CITY_CODE\":\"" \
                   + city_code + "\", \"BAR_CODE\":\"" + bar_code + "\",\"items\":["
            s_tmp = "{\"ft_code\":\"00\",\"ft_name\":\"总分数\",\"ft_score\":" + str(score) + ",\"ft_dline\":null," +\
                    "\"ft_uline\":null,\"ft_mean\":null,\"p_ft\":\"\",\"ft_hz\":\"\"," +\
                    "\"ft_level\":0,\"ft_sort\":null}"
            json_str = json_str + s_tmp
            s_first = ""
            s_second = ""
            tmp_code = ""
            # first_sort = 1
            # second_sort = 1
            # percent = ['FACTOR_XQMZL','FACTOR_XLZB','FACTOR_XLJZD','FACTOR_SCFE','FACTOR_XPXLZB','FACTOR_PHL',
            #            'FACTOR_CGL','FACTOR_DGL','FACTOR_LSHSB','FACTOR_HJXLB','FACTOR_DGM','FACTOR_DZM',
            #            'FACTOR_XHZS','FACTOR_DZL']
            for row in json_rec:
                first_code = row[6]
                second_code = row[12]
                if tmp_code == first_code:
                    pass
                else:
                    first_sort_df = sort_df[sort_df['FT_CODE'] == first_code]
                    # p_ft = first_sort_df['FT_CODE_P'].values[0]
                    # ft_hz = first_sort_df['FT_POSTFIX'].values[0]
                    ft_level = first_sort_df['LEVEL'].values[0]
                    ft_sort = first_sort_df['SORT'].values[0]
                    s_first = s_first + ",{\"ft_code\":\"" + row[6] + "\",\"ft_name\":\"" + row[7] + "\",\"ft_score\":" + str(row[8]) + "," +\
                              "\"ft_dline\":" + str(row[9]) + ",\"ft_uline\":" + str(row[10]) + ",\"ft_mean\":" + str(row[11]) + "," +\
                              "\"p_ft\":\"00\",\"ft_hz\":\"\",\"ft_level\":" + str(ft_level) + ",\"ft_sort\":" + str(ft_sort) + "}"
                    tmp_code = first_code
                pass
                second_sort_df = sort_df[sort_df['FT_CODE'] == second_code]
                # p_ft = second_sort_df['FT_CODE_P'].values[0]
                ft_hz = second_sort_df['FT_POSTFIX'].values[0]
                ft_level = second_sort_df['LEVEL'].values[0]
                ft_sort = second_sort_df['SORT'].values[0]
                hz = "\"" + ft_hz + "\""

                s_second = s_second + ",{\"ft_code\":\"" + row[12] + "\",\"ft_name\":\"" + row[13] + "\",\"ft_score\":" + str(row[14]) + "," + \
                           "\"ft_value\":" + str(row[15]) + ",\"ft_s_dline\":" + str(row[16]) + ",\"ft_s_uline\":" + str(row[17]) + "," +\
                           "\"ft_s_mean\":" + str(row[18]) + ",\"ft_dline\":" + str(row[19]) + ",\"ft_uline\":" + str(row[20]) + "," + \
                           "\"ft_mean\":" + str(row[21]) + ",\"p_ft\":\"" + row[6] + "\",\"ft_hz\":" + hz + ",\"ft_level\":" + str(ft_level) + "," +\
                           "\"ft_sort\":" + str(ft_sort) + "}"
            pass
            json_str = json_str + s_first
            json_str = json_str + s_second + "]}"
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",s_ft_sql)
        finally:
            db.close()
        pass
        return json_str
    pass
    ##########################################
    # 数据预处理
    # 反向指标和缺失值处理
    # 0:缺失值为1,1:删除缺失值
    ##########################################
    def preProcessData(self, group_code="", data=None):
        if data is None:
            return None
        pass
        # 1.0 查询指标项
        rows = self.getFeatures(group_code)

        # 2.0 反向指标处理
        # 需求满足率(FACTOR_XQMZL),销量排名(FACTOR_XLPM),商业存销比(FACTOR_SYCXB)
        # 社会存销比(FACTOR_SHCXB),销量集中度(FACTOR_XLJZD)
        reverse = self.getReverseFeatures(group_code)
        for row in rows:
            col =  row[3]
            if col in reverse:
                data.loc[data.SECOND_FT_CODE == col, 'SECOND_UPD_VALUE'] = \
                    data.loc[data.SECOND_FT_CODE == col, 'SECOND_UPD_VALUE'].apply(lambda x: 1.0/x if x != 0.0 else x)
            pass
        pass
        return data
    pass

pass
if __name__ == "__main__":
    model = RASSimulate()
    # model.mainFT('jszy_3','11110001', '201909','6901028111546')
    # model.mainScore('jszy_3','11110001', '201909','6901028111546')
    # model.mainFT('gdzy_7','11440101', '201909','6901028001625')
    # model.mainFT('gdzy_6','11440101', '201909','6901028004459')
    # model.mainFT('hubeizy_1','11440101', '201909','6901028184250')
    #
    # model.mainScore('gdzy_7','11440101', '201909','6901028001625')
    # model.mainScore('gdzy_6','11440101', '201909','6901028004459')
    model.mainFT('hubeizy_1','11420101', '201910','6901028184250')
    model.mainScore('hubeizy_1','11420101', '201910','6901028184250')
    # model.mainFT('hubeizy_1','11440101', '201910','6901028184250')
    # model.mainScore('hubeizy_1','11440101', '201910','6901028184250')
    # model.mainFT('guizhouzy_1','11440101', '201910','6901028102810')
    # model.mainScore('guizhouzy_1','11440101', '201910','6901028102810')
pass
