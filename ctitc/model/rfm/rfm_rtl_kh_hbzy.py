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

#########################################################
# 价值客户筛选评估分析模型
# RFM模型
#########################################################
class RFMRtlKhHBZY(RFMBase):
    MAX_RATIO = 1.5
    RATIOS = [1.1,1.2,1.3,1.4,1.5]
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
    def doProcess(self, corp_code='', corp_name='', group_code='',group_name='', ratio=None):
        # 1.遍历任务,进行处理
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

            # 0.数据预处理,除掉中烟提供数据的重复记录
            self.preProcess(corp_name, tbl_2)
            # 1.从大数据平台获取核心客户信息
            all_df = self.getRtlInfoFromHive(self.busidate,tbl_1)
            # 2.更新核心客户信息库,将当月新增核心户插入数据库
            self.updateRtl(all_df, tbl_2,tbl_3)
            # 3.1 数据预处理,去除异常值,准备CV计算
            cv_data = self.preProcessData(all_df)
            # 3.2 计算价值客户CV
            cv_df = self.countRtlCV(cv_data, corp_code,corp_name,group_code)
            # 4.1各中烟核心客户信息匹配
            self.matchRtl(all_df,cv_df, corp_code,corp_name,group_code,tbl_2,tbl_3)
            # 4.2 各中烟价值客户推荐
            self.recomRtl(all_df, cv_df, corp_code,corp_name,group_code)
            # 5.零售户价值特征表(分地市)
            self.doCityFeature(corp_code,corp_name,group_code,tbl_1)
            # 6.拜访人绩效(分地市)
            self.doVisitorRevalue(corp_code,corp_name,group_code,self.busidate,tbl_2)
            # 7.各中烟推荐价值客户匹配(分地市)
            self.doRecomRtlMatch(corp_code, corp_name, group_code, self.busidate,tbl_2)
            # 8. 各中烟拜访人匹配绩效表(分地市)
            self.doVistorRtlMatch(corp_code, corp_name, group_code, self.busidate)
        except Exception as ex:
            self.logger.error(str(ex))
        pass
    pass

    ##########################################
    # 中烟维护的价值客户预处理
    # 1.客户编码重复 2.许可证号重复 3.名称+地址重复
    # keep='first'表示保留第一次出现的重复行，是默认值。
    # keep另外两个取值为"last"和False，分别表示保留最后一次出现的重复行和去除所有重复行
    ##########################################
    def preProcess(self, corp_name='', tbl_2=''):
        db = MysqlDB(logger=self.logger)
        tbl_err = tbl_2 + "_ERR"
        try:
            db.connect()
            # 0 去除异常字符
            u_sql = "update " + tbl_2 + " set R_ADDRESS = trim(replace(R_ADDRESS,'''','')), " \
                                        " R_NAME = trim(replace(R_NAME,'''','')), " \
                                        " VISIT_CODE = trim(replace(VISIT_CODE,'''','')), " \
                                        " VISIT_NAME = trim(replace(VISIT_NAME,'''','')), " \
                                        " R_CODE = trim(replace(R_CODE,'''','')), " \
                                        " R_CONTACTOR = trim(replace(R_CONTACTOR,'''','')), " \
                                        " R_TEL = trim(replace(R_TEL,'''','')), " \
                                        " R_SLH = trim(replace(R_SLH,'''','')), " \
                                        " R_XKZH = trim(replace(R_XKZH,'''','')), " \
                                        " R_LABEL = trim(replace(R_LABEL,'''','')), " \
                                        " DEPT_NAME = trim(replace(DEPT_NAME,'''','')) " \
                                        " where BUSI_DATE = '" + self.busidate + "' "
            ret = db.update(u_sql)
            self.logger.info(" 核心户业务表update Ok= %s", str(ret))
            # 1 拜访类别:0:内部人员,1:外部人员
            u_sql = "update " + tbl_2 + " set VISIT_TYPE = case when substr(VISIT_CODE,1,2)='99' or " \
                                        " substr(VISIT_CODE,1,2)='98'  then '1' else '0' end " \
                                        " where BUSI_DATE = '" + self.busidate + "' "
            ret = db.update(u_sql)
            self.logger.info(" 核心户VISIT_TYPE update Ok= %s", str(ret))
            # 1 插入前先删除核心户信息错误表
            d_sql = "delete from " + tbl_err + " where BUSI_DATE = '" + self.busidate + "' "
            ret = db.delete(d_sql)
            self.logger.info(" 核心户信息错误表删除成功= %s", corp_name + " - " + self.busidate + "-" + str(ret))
            # 按省循环处理
            s_sql = "SELECT distinct PROV_CODE FROM  " + tbl_2 + " WHERE  BUSI_DATE='" + self.busidate + "'"
            provs = db.select(s_sql)
            count = 0
            for row in provs:
                prov_code = row[0]
                s_sql = "SELECT BUSI_DATE, trim(VISIT_CODE) VISIT_CODE, VISIT_NAME, VISIT_TYPE, PROV_CODE, PROV_NAME,CITY_CODE, " + \
                        " CITY_NAME, DEPT_NAME, trim(R_CODE) R_CODE,trim(R_NAME) R_NAME, trim(R_ADDRESS) R_ADDRESS,  " \
                        " trim(R_XKZH) R_XKZH, R_CONTACTOR, R_TEL,R_SLH,R_LABEL " \
                        " FROM  " + tbl_2 + " WHERE  BUSI_DATE='" + self.busidate + "'  and PROV_CODE='" + prov_code +"'" + \
                        " order by PROV_CODE, CITY_CODE"
                zy_rec = db.select(s_sql)
                self.logger.info(" SQL=:\n%s",s_sql)

                zy_df = pd.DataFrame(list(zy_rec), columns=['BUSI_DATE', 'VISIT_CODE', 'VISIT_NAME', 'VISIT_TYPE','PROV_CODE','PROV_NAME',
                                                            'CITY_CODE', 'CITY_NAME', 'DEPT_NAME','R_CODE','R_NAME',
                                                            'R_ADDRESS', 'R_XKZH', 'R_CONTACTOR','R_TEL','R_SLH','R_LABEL'])
                self.logger.info(" zy_df=%s",len(zy_df))
                zy_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE','PROV_NAME','CITY_CODE', 'CITY_NAME',
                                              'DEPT_NAME','R_CODE','R_NAME','R_ADDRESS', 'R_XKZH', 'R_CONTACTOR',
                                              'R_TEL','R_SLH','R_LABEL'], inplace=True)
                self.logger.info(" zy_df=%s",len(zy_df))
                # 1.客户编码重复
                code_df = zy_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'],
                                                keep=False, inplace=False)
                code_df = zy_df.append(code_df).drop_duplicates(keep=False, inplace=False)
                self.logger.info(" code_df=%s",len(code_df))
                count = count + len(code_df)
                # 插入数据库
                if len(code_df) > 0 :
                    i_sql = self.getSql(code_df, tbl_err, '0')
                    ret = db.insert(i_sql)
                pass
                # 2.许可证号重复
                xkzh_df = zy_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_XKZH'],
                                                keep=False, inplace=False)
                xkzh_df = zy_df.append(xkzh_df).drop_duplicates(keep=False, inplace=False)
                self.logger.info(" xkzh_df=%s",len(xkzh_df))
                count = count + len(xkzh_df)
                xkzh_df = xkzh_df.dropna(subset=['R_XKZH'])
                xkzh_df = xkzh_df[xkzh_df['R_XKZH'] != '']
                self.logger.info(" xkzh_df=%s",len(xkzh_df))
                # 插入数据库
                if len(xkzh_df) > 0 :
                    i_sql = self.getSql(xkzh_df, tbl_err, '1')
                    ret = db.insert(i_sql)
                pass
                # 3.名称+地址重复重复
                name_df = zy_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_NAME', 'R_ADDRESS'],
                                                keep=False, inplace=False)
                name_df = zy_df.append(name_df).drop_duplicates(keep=False, inplace=False)
                # name_df = name_df.append(code_df).drop_duplicates(keep=False, inplace=False)
                name_df = name_df.dropna(subset=['R_NAME','R_ADDRESS'])
                name_df = name_df[name_df['R_NAME'] != '']
                self.logger.info(" name_df=%s",len(name_df))
                count = count + len(name_df)
                # 插入数据库
                if len(name_df) > 0 :
                    i_sql = self.getSql(name_df, tbl_err, '2')
                    ret = db.insert(i_sql)
                pass
            pass
            self.logger.info("错误零售户处理成功记录数 = :%s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
        finally:
            db.close()
        pass
    pass
    ##########################################
    # 获取SQL
    ##########################################
    def getSql(self, data,tbl='', err_type='0'):
        if data is None:
            return
        pass
        i_sql = ""
        count = 0
        PRE_SQL = "INSERT INTO " + tbl + " (BUSI_DATE, VISIT_CODE, VISIT_NAME,VISIT_TYPE, PROV_CODE, PROV_NAME, " + \
                  " CITY_CODE,CITY_NAME,DEPT_NAME,R_CODE,R_NAME,R_ADDRESS,ERR_TYPE,R_CONTACTOR,R_TEL,R_SLH," + \
                  " R_XKZH,R_LABEL) VALUES "
        for row in data.index:
            busi_date = data.loc[row,'BUSI_DATE']
            visit_code = data.loc[row,'VISIT_CODE']
            visit_name = data.loc[row,'VISIT_NAME']
            visit_type = data.loc[row,'VISIT_TYPE']
            prov_code = data.loc[row,'PROV_CODE']
            pro_name = data.loc[row,'PROV_NAME']
            city_code = data.loc[row,'CITY_CODE']
            city_name = data.loc[row,'CITY_NAME']
            dept_name = data.loc[row,'DEPT_NAME']
            r_code = data.loc[row,'R_CODE']
            r_name = data.loc[row,'R_NAME']
            r_add = data.loc[row,'R_ADDRESS']
            r_contactor = data.loc[row,'R_CONTACTOR']
            r_tel = data.loc[row,'R_TEL']
            r_slh = data.loc[row,'R_SLH']
            r_xkzh = data.loc[row,'R_XKZH']
            r_label = data.loc[row,'R_LABEL']
            i_sql = i_sql + " ('" + busi_date + "', '" + visit_code + "', '" + visit_name + "', '" + visit_type + "', '" + \
                    prov_code + "', '" + pro_name + "', '" + city_code + "', '"  + city_name + "','" + \
                    dept_name + "', '" + r_code + "', '" + r_name + "', '"  + r_add + "','" + \
                    err_type + "', '" + r_contactor + "', '" + r_tel + "', '"  + r_slh + "','" + \
                    r_xkzh + "', '" + r_label + "'),"
            count = count + 1
        pass
        if (i_sql != ""):
            i_sql = i_sql.rstrip(',')
            i_sql = PRE_SQL + i_sql
        pass
        return i_sql
    pass
    ##########################################
    # 数据预处理
    # 异常值处理
    # 0:缺失值为1,1:删除缺失值
    ##########################################
    def preProcessData(self, all_df):
        if all_df is None:
            return None
        pass
        data = all_df[['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_KHTYBM', 'F_NUM', 'M_NUM', 'X_NUM','LSH_GGS']]
        data = data[data['X_NUM'] > 0.0]
        data['R_DX'] = data.apply(lambda x: x['M_NUM'] / x['X_NUM'], axis=1)
        data[['X_NUM','R_DX','F_NUM','LSH_GGS']] = data[['X_NUM','R_DX','F_NUM','LSH_GGS']].astype('float')
        self.logger.info(" data=%s",len(data))
        self.logger.info(" data=%s", data.columns)

        # 1.0 其余缺失值处理
        data.fillna(0.0, inplace=True)

        # # 2.0 异常值处理处理, 按地市/省份进行异常值处理
        # codes_df = data[['CITY_CODE']]
        # city_codes = codes_df.drop_duplicates(subset=['CITY_CODE'], inplace=False)
        # for row in city_codes.index:
        #     city_code = city_codes.loc[row,'CITY_CODE']
        #     city_df = data[data['CITY_CODE']==city_code]
        #     city_stat = self.getStatics(city_df.drop(city_df.columns[0:4], axis=1, inplace=False))
        #
        #     # 2.1 异常值处理
        #     cols = city_stat.columns
        #     for col in cols:
        #         up_value = city_stat.loc['uline',col]
        #         down_value = city_stat.loc['dline',col]
        #         data.loc[data.CITY_CODE == city_code, col] = \
        #             data.loc[data.CITY_CODE == city_code, col].apply(lambda x: up_value if x >= up_value else down_value if x <= down_value else x)
        #     pass
        # pass
        return data
    pass

    ##########################################
    # 计算价值客户CV并推荐
    ##########################################
    def countRtlCV(self, data, corp_code, corp_name, group_code):
        if data is None:
            return None
        pass
        # 1.0 cv值计算,以地市为单位
        codes_df = data[['PROV_CODE', 'CITY_CODE']]
        city_codes = codes_df.drop_duplicates(subset=['PROV_CODE', 'CITY_CODE'], inplace=False)

        db = MysqlDB(logger=self.logger)
        i_sql = ""
        arr_code = []
        try:
            db.connect()
            # 1.1 计算指标权重表
            # 1.1.1 插入前先删除指标权重表
            d_sql = "delete from RFM_CITY_WEIGHT where BUSI_DATE = '" + self.busidate + "' " +\
                    " and CORP_CODE = '" + corp_code + "' and GROUP_CODE = '" + group_code + "'"
            ret = db.delete(d_sql)
            self.logger.info(" 地市权重月表删除成功= %s", corp_name + " - " + self.busidate + "-" + str(ret))
            count = 0
            PRE_SQL = "INSERT INTO RFM_CITY_WEIGHT (BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, " +\
                      " PROV_CODE, CITY_CODE, FT_CODE, CV_VALUE, CV_WEIGHT, MEAN_VALUE, " +\
                      " MAX_VALUE,MIN_VALUE) VALUES "
            for row in city_codes.index:
                prov_code = city_codes.loc[row,'PROV_CODE']
                city_code = city_codes.loc[row,'CITY_CODE']
                city_df = data[data['CITY_CODE']==city_code]
                city_stat = self.getStatics(city_df.drop(city_df.columns[0:4], axis=1, inplace=False))

                # 1.1 求CV值的和
                cols = city_stat.columns
                sum_value = 0.0
                for col in cols:
                    cv_value = city_stat.loc['cv',col]
                    sum_value = sum_value + cv_value
                pass
                # 1.2 求CV值的权重
                for item in city_df.index:
                    r_khtym = city_df.loc[item,'R_KHTYBM']
                    # 计算总分
                    z_score = 0.0
                    for col in cols:
                        cv_value = city_stat.loc['cv',col]
                        cv_weight = cv_value / sum_value
                        mean = city_stat.loc['mean',col]
                        max = city_stat.loc['max',col]
                        min = city_stat.loc['min',col]
                        if (max - min) != 0.0 :
                            z_value = (city_df.loc[item, col] - min) /(max - min)
                            z_score = z_score + (z_value * 100.0) * cv_weight
                        else:
                            # first_score = "null"
                            pass
                        pass
                    pass
                    arr_code.append([city_code, r_khtym, z_score])
                pass

                # 1.3 将CV值保存到数据库
                for col in cols:
                    cv_value = city_stat.loc['cv',col]
                    cv_weight = cv_value / sum_value
                    mean = city_stat.loc['mean',col]
                    max = city_stat.loc['max',col]
                    min = city_stat.loc['min',col]

                    count = count + 1
                    i_sql = i_sql + " ('" + self.busidate + "', '" + corp_code + "', '" + corp_name + "', '" \
                            + group_code + "', '" + prov_code + "', '" + city_code + "', '" \
                            + col + "', " + str(cv_value)+ "," + str(cv_weight)+ "," + \
                            str(mean) + ","  +  str(max)+ ", "  +  str(min)+ " ),"
                    if count%2000 == 0:
                        i_sql = i_sql.rstrip(',')
                        i_sql = PRE_SQL + i_sql
                        ret = db.insert(i_sql)
                        i_sql = ""
                    pass
                pass
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                # self.logger.info("  i_sql= :%s", i_sql)
                ret = db.insert(i_sql)
            pass
            self.logger.info(" 成功插入地市权重月表(RFM_CITY_WEIGHT)记录数 = :%s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
        finally:
            db.close()
        pass
        self.logger.info(" arr_code=%s",len(arr_code))
        cv_df = pd.DataFrame(arr_code, columns=['CITY_CODE','R_KHTYBM','Z_SCORE'])
        cv_df.sort_values(by=['CITY_CODE', 'Z_SCORE'], ascending=False, inplace=True)
        return cv_df
    pass
    ##########################################
    # 各中烟价值客户推荐
    ##########################################
    def recomRtl(self, all_df, cv_df, corp_code, corp_name, group_code):
        bfzs_df = None
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            # 1.计算实际拜访的总户数
            # 1.2 核心户
            s_sql = "SELECT PROV_CODE, CITY_CODE, count(distinct R_KHTYBM) BF_NUM " + \
                    " FROM RFM_CORP_RTL_VALID " + \
                    " WHERE BUSI_DATE='" + self.busidate + "' and CORP_CODE='" + corp_code + "' and " + \
                    " GROUP_CODE='" + group_code + "' " + \
                    " group by PROV_CODE, CITY_CODE "
            rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            bfzs_df = pd.DataFrame(list(rec), columns=['PROV_CODE','CITY_CODE','BF_NUM'])
            bfzs_df.fillna(0.0, inplace=True)

            # 2.0 插入前先删除价值客户推荐表
            d_sql = "delete from RFM_VALUE_RTL_RECOM where BUSI_DATE = '" + self.busidate + "' " +\
                    " and CORP_CODE = '" + corp_code + "' and GROUP_CODE = '" + group_code + "'"
            ret = db.delete(d_sql)
            self.logger.info(" 价值客户推荐表删除成功= %s", corp_name + " - " + self.busidate + "-" + str(ret))

            # 3.0 以地市为单位,推荐零售户
            codes_df = bfzs_df[['PROV_CODE', 'CITY_CODE']]
            city_codes = codes_df.drop_duplicates(subset=['PROV_CODE', 'CITY_CODE'], inplace=False)
            count = 0
            for row in city_codes.index:
                prov_code = city_codes.loc[row,'PROV_CODE']
                city_code = city_codes.loc[row,'CITY_CODE']
                city_cv_df = cv_df[cv_df['CITY_CODE']==city_code]
                city_all_df = all_df[all_df['CITY_CODE']==city_code]
                # city_cv_df排序
                city_cv_df.sort_values(by=['Z_SCORE'], ascending=False, inplace=True)
                # 实际拜访的总户数
                bfzs = bfzs_df[bfzs_df['CITY_CODE']==city_code]['BF_NUM'].values[0]
                recom_num = int(bfzs * self.MAX_RATIO) + 1
                recom_df = city_cv_df[0:recom_num]

                valid_df = pd.merge(recom_df,city_all_df,on=['CITY_CODE', 'R_KHTYBM'], how='inner')
                valid_df = valid_df.drop_duplicates(subset=['PROV_CODE', 'CITY_CODE', 'R_KHTYBM'],
                                                    keep='first',inplace=False)

                # self.logger.info(" valid_df=%s", len(valid_df))
                # self.logger.info(" valid_df=%s", valid_df.columns)
                PRE_SQL = "INSERT INTO RFM_VALUE_RTL_RECOM(BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, PROV_CODE," \
                          " PROV_NAME, CITY_CODE,CITY_NAME,R_KHTYBM,R_KHBM,R_XKZH, R_NAME,R_ADDRESS,R_JYYT,R_CXFL," \
                          " R_DW,R_CONTACTOR,R_TEL,R_NUM,R_RANK, F_NUM,F_RANK,M_NUM,M_RANK,X_NUM,X_RANK,LSH_GGS, ZJE," \
                          " ZJE_RANK,ZDX,ZDX_RANK,Z_SCORE,Z_RANK) VALUES "
                i_sql = ""
                values = {'R_XKZH': '', 'R_NAME': '', 'R_ADDRESS': '', 'R_JYYT': '',
                          'R_CXFL': '', 'R_DW': '', 'R_CONTACTOR': '', 'R_TEL': ''}
                valid_df.fillna(value=values, inplace=True)
                valid_df.fillna(0, inplace=True)
                no_seq = 1
                for row in valid_df.index:
                    busi_date = self.busidate
                    prov_code = valid_df.loc[row,'PROV_CODE']
                    pro_name = valid_df.loc[row,'PROV_NAME']
                    city_code = valid_df.loc[row,'CITY_CODE']
                    city_name = valid_df.loc[row,'CITY_NAME']
                    r_khtym = valid_df.loc[row,'R_KHTYBM']
                    r_code = valid_df.loc[row,'R_CODE']
                    r_xkzh = valid_df.loc[row,'R_XKZH']
                    r_name = valid_df.loc[row,'R_NAME']
                    r_add = valid_df.loc[row,'R_ADDRESS']
                    if r_khtym == '341601008388':
                        r_add = "城关镇阜蒙路"
                    pass
                    r_jyty = valid_df.loc[row,'R_JYYT']
                    r_cxfl = valid_df.loc[row,'R_CXFL']
                    r_dw = valid_df.loc[row,'R_DW']
                    r_contactor = valid_df.loc[row,'R_CONTACTOR']
                    r_tel = valid_df.loc[row,'R_TEL']
                    r_num = str(valid_df.loc[row,'R_NUM'])
                    r_rank = str(valid_df.loc[row,'R_RANK'])
                    r_f_num = str(valid_df.loc[row,'F_NUM'])
                    r_f_rank = str(valid_df.loc[row,'F_RANK'])
                    r_m_num = str(valid_df.loc[row,'M_NUM'])
                    r_m_rank = str(valid_df.loc[row,'M_RANK'])
                    r_x_num = str(valid_df.loc[row,'X_NUM'])
                    r_x_rank = str(valid_df.loc[row,'X_RANK'])
                    r_lsh_ggs = str(valid_df.loc[row,'LSH_GGS'])
                    r_zje = str(valid_df.loc[row,'ZJE'])
                    r_zje_rank = str(valid_df.loc[row,'ZJE_RANK'])
                    r_zdx = str(valid_df.loc[row,'ZDX'])
                    r_zdx_rank = str(valid_df.loc[row,'ZDX_RANK'])
                    r_score = str(valid_df.loc[row,'Z_SCORE'])
                    r_rank = str(row + 1)

                    i_sql = i_sql + " ('" + busi_date + "', '" + corp_code + "', '" + corp_name + "', '" \
                            + group_code + "', '" + prov_code + "', '" + pro_name + "', '" \
                            + city_code + "', '" + city_name + "', '" + r_khtym + "','"  + r_code + "', '" \
                            + r_xkzh + "', '" + r_name + "', '" + r_add + "','"  + r_jyty + "', '" \
                            + r_cxfl + "', '" + r_dw + "', '" + r_contactor + "', '" + r_tel + "',"  + r_num + ", " \
                            + r_rank + "," + r_f_num + "," + r_f_rank + ","  + r_m_num + "," \
                            + r_m_rank + "," + r_x_num + "," + r_x_rank + ","  + r_lsh_ggs + ","  + r_zje + "," \
                            + r_zje_rank + "," + r_zdx + "," + r_zdx_rank + "," + r_score + "," + r_rank + "),"
                    count = count + 1
                    if count%1000 == 0:
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
            pass
            self.logger.info(" 价值客户推荐成功记录数 = %s",count)

            # 4.0 更新对推荐匹配户（推荐户和拜访户的重合户）打上标识；2）补充拜访人工号和姓名
            u_sql = "update RFM_VALUE_RTL_RECOM a, RFM_CORP_RTL_VALID b set a.VISIT_CODE = b.VISIT_CODE, " + \
                    " a.VISIT_TYPE = b.VISIT_TYPE, a.VISIT_NAME = b.VISIT_NAME, a.MATCH_TYPE='1' " + \
                    " where a.PROV_CODE=b.PROV_CODE and a.CITY_CODE=b.CITY_CODE and " + \
                    " a.R_KHTYBM=b.R_KHTYBM and a.CORP_CODE=b.CORP_CODE and a.GROUP_CODE=b.GROUP_CODE and " + \
                    " a.CORP_CODE= '" + corp_code + "' and a.GROUP_CODE= '" + group_code + "' and " + \
                    " a.BUSI_DATE=b.BUSI_DATE and b.BUSI_DATE = '" + self.busidate + "' "
            ret = db.update(u_sql)
            self.logger.info("价值客户推荐表update Ok= %s", " RFM_VALUE_RTL_RECOM " + str(ret))
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
        finally:
            db.close()
        pass
    pass
    ##########################################
    # 更新核心客户信息库,将当月新增核心户插入数据库
    ##########################################
    def updateRtl(self, all_df, tbl_2,tbl_3):
        # 1.信息更新
        db = MysqlDB(logger=self.logger)
        country_df = all_df[['BUSI_DATE', 'PROV_CODE', 'PROV_NAME', 'CITY_CODE', 'CITY_NAME',
                             'R_KHTYBM', 'R_CODE', 'KHBH_DQ', 'R_XKZH','R_NAME', 'R_ADDRESS']]
        i_sql = ""
        try:
            db.connect()
            # 自有户
            zyh_sql = "SELECT BUSI_DATE, PROV_CODE, CITY_CODE, " + \
                      " DEPT_NAME, R_CODE, R_NAME, R_ADDRESS, R_XKZH, R_CONTACTOR," + \
                      " R_TEL, R_SLH, R_LABEL  FROM  " + tbl_2 + " WHERE  BUSI_DATE='" + self.busidate + "' " + \
                      " order by PROV_CODE, CITY_CODE"
            zyh_rec = db.select(zyh_sql)
            self.logger.info(" SQL=:\n%s",zyh_sql)
            zyh_df = pd.DataFrame(list(zyh_rec), columns=['BUSI_DATE', 'PROV_CODE','CITY_CODE',
                                                          'DEPT_NAME','R_CODE','R_NAME','R_ADDRESS', 'R_XKZH',
                                                          'R_CONTACTOR','R_TEL','R_SLH','R_LABEL'])
            self.logger.info(" zyh_df=%s",len(zyh_df))
            zyh_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'], inplace=True)
            self.logger.info(" zyh_df=%s",len(zyh_df))

            # 核心户
            s_sql = "SELECT a.BUSI_DATE, a.PROV_CODE, a.CITY_CODE, " + \
                    " a.DEPT_NAME, a.R_CODE,a.R_NAME, a.R_ADDRESS,a.R_XKZH, a.R_CONTACTOR," + \
                    " a.R_TEL, a.R_SLH, a.R_LABEL  " + \
                    " FROM  " + tbl_2 + " a," + tbl_3 + " b  WHERE  a.BUSI_DATE='" + self.busidate + "' " + \
                    " and a.CITY_CODE = b.CITY_CODE and a.R_CODE = b.R_CODE " + \
                    " order by a.PROV_CODE, a.CITY_CODE"
            hxh_rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)

            hxh_df = pd.DataFrame(list(hxh_rec), columns=['BUSI_DATE', 'PROV_CODE','CITY_CODE',
                                                          'DEPT_NAME','R_CODE','R_NAME','R_ADDRESS', 'R_XKZH',
                                                          'R_CONTACTOR','R_TEL','R_SLH','R_LABEL'])
            self.logger.info(" hxh_df=%s",len(hxh_df))
            hxh_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'], inplace=True)
            self.logger.info(" hxh_df=%s",len(hxh_df))
            zy_all_df = zyh_df.append(hxh_df)
            zy_all_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'],keep=False, inplace=True)
            self.logger.info(" zy_all_df=%s",len(zy_all_df))
            # 1.按省循环处理新增零售户
            codes_df = zy_all_df[['PROV_CODE']]
            prov_codes = codes_df.drop_duplicates(subset=['PROV_CODE'], inplace=False)
            for row in prov_codes.index:
                prov_code = prov_codes.loc[row,'PROV_CODE']
                self.logger.info(" prov_code=%s",prov_code)

                zy_df = zy_all_df[zy_all_df['PROV_CODE'] == prov_code]
                prov_df = country_df[country_df['PROV_CODE'] == prov_code]

                # 去重:一个用户拜访多个零售户VISIT_CODE:R_CODE
                zy_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'],inplace=True)
                self.logger.info(" prov_df=%s",len(prov_df))
                self.logger.info(" prov_df=%s",prov_df.columns)
                self.logger.info(" zy_df=%s",len(zy_df))
                self.logger.info(" zy_df=%s",zy_df.columns)

                # 1.1地市客户统一编码R_KHBM=R_CODE
                code_df = pd.merge(zy_df, prov_df.drop(['R_XKZH','R_NAME', 'R_ADDRESS','KHBH_DQ'], axis=1, inplace=False),
                                   how='inner', on=['BUSI_DATE','PROV_CODE','CITY_CODE','R_CODE'])
                self.logger.info(" code_df=%s",code_df.columns)
                self.logger.info(" code_df=%s",len(code_df))
                # 去重
                code_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'],inplace=True)
                self.logger.info(" 去重code_df=%s",len(code_df))

                # 1.2许可证R_XKZH=R_XKZH
                # 去除空值
                zy_df_n = zy_df.dropna(subset=['R_XKZH'])
                zy_df_n = zy_df_n[zy_df_n['R_XKZH'] != '']
                prov_df_n = prov_df.dropna(subset=['R_XKZH'])
                prov_df_n = prov_df_n[prov_df_n['R_XKZH'] != '']
                xkz_df = pd.merge(zy_df_n, prov_df_n.drop(['R_CODE', 'R_NAME', 'R_ADDRESS','KHBH_DQ'], axis=1, inplace=False),
                                  how='inner', on=['BUSI_DATE','PROV_CODE','CITY_CODE','R_XKZH'])
                self.logger.info(" xkz_df=%s",xkz_df.columns)
                self.logger.info(" xkz_df=%s",len(xkz_df))

                # 1.3零售户名称+零售户地址
                # 去除空值
                zy_df_n = zy_df.dropna(subset=['R_NAME','R_ADDRESS'])
                zy_df_n = zy_df_n[zy_df_n['R_NAME'] != '']
                zy_df_n = zy_df_n[zy_df_n['R_ADDRESS'] != '']
                prov_df_n = prov_df.dropna(subset=['R_NAME','R_ADDRESS'])
                prov_df_n = prov_df_n[prov_df_n['R_NAME'] != '']
                prov_df_n = prov_df_n[prov_df_n['R_ADDRESS'] != '']

                prov_df_n.rename(columns={'R_ADDRESS':'R_ADDRESS_1'},inplace=True)
                name_add_df = pd.merge(zy_df_n, prov_df_n.drop(['R_CODE', 'R_XKZH', 'KHBH_DQ'], axis=1, inplace=False),
                                       how='inner', on=['BUSI_DATE','PROV_CODE','CITY_CODE','R_NAME'])
                self.logger.info(" name_add_df=%s",len(name_add_df))
                self.logger.info(" name_add_df=%s",name_add_df.columns)
                name_df = name_add_df[name_add_df['R_ADDRESS'] == name_add_df['R_ADDRESS_1']]
                add_df = name_add_df[name_add_df['R_ADDRESS'] != name_add_df['R_ADDRESS_1']]
                self.logger.info(" name_df=%s",len(name_df))
                self.logger.info(" add_df=%s",len(add_df))
                for row in add_df.index:
                    prov_name = add_df.loc[row, 'PROV_NAME']
                    city_code = add_df.loc[row, 'CITY_CODE']
                    city_name = add_df.loc[row, 'CITY_NAME']
                    addr1 = add_df.loc[row, 'R_ADDRESS']
                    addr2 = add_df.loc[row, 'R_ADDRESS_1']

                    addr1 = addr1.replace(prov_name,'',1)
                    addr2 = addr2.replace(prov_name,'',1)
                    sim = self.countSimilary(addr1, addr2)
                    if sim < 0.8:
                        add_df.drop(row,axis=0,inplace=True)
                    pass
                pass
                name_df = name_df.append(add_df,ignore_index=True)
                name_df.drop(['R_ADDRESS_1'], axis=1, inplace=True)
                self.logger.info(" final name_df=%s",name_df.columns)
                self.logger.info(" final name_df=%s",len(name_df))
                prov_df_n.rename(columns={'R_ADDRESS_1':'R_ADDRESS'},inplace=True)

                # 浙江省用KHBH_DQ=R_CODE
                zj_df = None
                if prov_code == '11330001':
                    # 地市客户统一编码R_KHBM=R_CODE
                    tmp_df = prov_df.drop(['R_XKZH','R_NAME', 'R_ADDRESS', 'R_CODE'], axis=1, inplace=False)
                    tmp_df.rename(columns={'KHBH_DQ':'R_CODE'},inplace=True)
                    zj_df = pd.merge(zy_df, tmp_df,
                                     how='inner', on=['BUSI_DATE','PROV_CODE','CITY_CODE','R_CODE'])
                pass

                # 取并集
                valid_df = pd.merge(code_df,xkz_df,on=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE', 'R_XKZH', 'R_NAME',
                                                       'R_ADDRESS', 'PROV_NAME', 'CITY_NAME', 'R_KHTYBM','DEPT_NAME',
                                                       'R_CONTACTOR','R_TEL','R_SLH','R_LABEL'], how='outer')
                valid_df = pd.merge(valid_df,name_df,on=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE', 'R_XKZH', 'R_NAME',
                                                         'R_ADDRESS', 'PROV_NAME', 'CITY_NAME', 'R_KHTYBM','DEPT_NAME',
                                                         'R_CONTACTOR','R_TEL','R_SLH','R_LABEL'], how='outer')
                # 浙江省用
                if prov_code == '11330001' and zj_df is not None:
                    valid_df = pd.merge(valid_df,zj_df,on=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE', 'R_XKZH', 'R_NAME',
                                                           'R_ADDRESS', 'PROV_NAME', 'CITY_NAME', 'R_KHTYBM','DEPT_NAME',
                                                           'R_CONTACTOR','R_TEL','R_SLH','R_LABEL'], how='outer')
                pass
                self.logger.info(" valid_df=%s",len(valid_df))
                valid_df = valid_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'],
                                                    keep='first',inplace=False)
                self.logger.info(" valid_df=%s",len(valid_df))
                valid_df = valid_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE','R_KHTYBM'],
                                                    keep='first',inplace=False)
                self.logger.info(" valid_df=%s",len(valid_df))
                # 插入数据库
                PRE_SQL = "INSERT INTO "+ tbl_3 + " (BUSI_DATE, PROV_CODE,PROV_NAME, CITY_CODE,CITY_NAME,R_KHTYBM," \
                                                  " R_CODE,R_XKZH, R_NAME,R_ADDRESS,DEPT_NAME,R_CONTACTOR,R_TEL, R_SLH,R_LABEL) VALUES "
                i_sql = ""
                count = 0
                values = {'R_XKZH': '', 'R_NAME': '', 'R_ADDRESS': '', 'DEPT_NAME': '',
                          'R_SLH': '', 'R_LABEL': '', 'R_CONTACTOR': '', 'R_TEL': ''}
                valid_df.fillna(value=values, inplace=True)
                valid_df.fillna(0, inplace=True)
                for row in valid_df.index:
                    busi_date = valid_df.loc[row,'BUSI_DATE']
                    prov_code = valid_df.loc[row,'PROV_CODE']
                    pro_name = valid_df.loc[row,'PROV_NAME']
                    city_code = valid_df.loc[row,'CITY_CODE']
                    city_name = valid_df.loc[row,'CITY_NAME']
                    r_khtym = valid_df.loc[row,'R_KHTYBM']
                    r_code = valid_df.loc[row,'R_CODE']
                    r_xkzh = valid_df.loc[row,'R_XKZH']
                    r_name = valid_df.loc[row,'R_NAME']
                    r_add = valid_df.loc[row,'R_ADDRESS']
                    r_dept = valid_df.loc[row,'DEPT_NAME']
                    r_slh = valid_df.loc[row,'R_SLH']
                    r_label = valid_df.loc[row,'R_LABEL']
                    r_contactor = valid_df.loc[row,'R_CONTACTOR']
                    r_tel = valid_df.loc[row,'R_TEL']

                    i_sql = i_sql + " ('" + busi_date + "', '" + prov_code + "', '" + pro_name + "', '" \
                            + city_code + "', '" + city_name + "', '" + r_khtym + "','"  + r_code + "', '" \
                            + r_xkzh + "', '" + r_name + "', '" + r_add + "','"  + r_dept + "', '" \
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
            pass
            # 2.用新的属性更新核心户属性
            u_sql = "update " + tbl_3 + " a, " + tbl_2 + " b set a.R_ADDRESS = b.R_ADDRESS, " + \
                    " a.R_NAME = b.R_NAME, a.R_CONTACTOR = b.R_CONTACTOR, " + \
                    " a.R_TEL = b.R_TEL, a.R_SLH = b.R_SLH, " + \
                    " a.R_XKZH = b.R_XKZH, a.R_LABEL = b.R_LABEL, " + \
                    " a.DEPT_NAME = b.DEPT_NAME " + \
                    " where a.PROV_CODE=b.PROV_CODE and a.CITY_CODE=b.CITY_CODE and " + \
                    " a.R_CODE=b.R_CODE and b.BUSI_DATE = '" + self.busidate + "' "
            ret = db.update(u_sql)
            self.logger.info("核心户主表update Ok= %s", tbl_3 + " " + str(ret))
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql=%s",i_sql)
        finally:
            db.close()
        pass
    pass
    ##########################################
    # 各中烟核心客户信息匹配
    ##########################################
    def matchRtl(self, all_df, cv_df, corp_code, corp_name, group_code,tbl_2, tbl_3):

        # 信息匹配
        db = MysqlDB(logger=self.logger)
        i_sql = ""
        try:
            db.connect()
            # 1.0 插入前先删除有效核心零售户信息表和无效核心零售户信息表
            # 删除有效核心零售户信息表
            d_sql = "delete from RFM_CORP_RTL_VALID where BUSI_DATE = '" + self.busidate + "' " +\
                       " and CORP_CODE = '" + corp_code + "' and GROUP_CODE = '" + group_code + "'"
            ret = db.delete(d_sql)
            self.logger.info(" 有效核心零售户信息表删除成功= %s", corp_name + " - " + self.busidate + "-" + str(ret))

            # 删除无效核心零售户信息表
            d_sql = "delete from RFM_CORP_RTL_ERR where BUSI_DATE = '" + self.busidate + "' " +\
                     " and CORP_CODE = '" + corp_code + "' and GROUP_CODE = '" + group_code + "'"
            ret = db.delete(d_sql)
            self.logger.info(" 无效核心零售户信息表删除成功= %s", corp_name + " - " + self.busidate + "-" + str(ret))

            # 2.0匹配获取R_KHTYBM
            s_sql = "SELECT a.BUSI_DATE, a.PROV_CODE, a.CITY_CODE, " + \
                    " a.R_CODE, a.R_XKZH, b.R_KHTYBM, a.R_NAME, a.R_ADDRESS, a.VISIT_CODE, a.VISIT_TYPE, a.VISIT_NAME " + \
                    " FROM  " + tbl_2 + " a," + tbl_3 + " b  WHERE  a.BUSI_DATE='" + self.busidate + "' " + \
                    " and a.CITY_CODE = b.CITY_CODE and a.R_CODE = b.R_CODE " + \
                    " order by a.PROV_CODE, a.CITY_CODE"
            zy_rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)

            zy_df = pd.DataFrame(list(zy_rec), columns=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE',
                                                        'R_XKZH','R_KHTYBM', 'R_NAME', 'R_ADDRESS',
                                                        'VISIT_CODE', 'VISIT_TYPE', 'VISIT_NAME'])

            # 去重:一个用户拜访多个零售户VISIT_CODE:R_CODE
            self.logger.info(" zy_df=%s",len(zy_df))
            zy_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'],inplace=True)
            self.logger.info(" 去重zy_df=%s",len(zy_df))
            self.logger.info(" zy_df=%s",zy_df.columns)

            # 获取当月有效量用户
            valid_df = pd.merge(zy_df, all_df.drop(['R_XKZH', 'R_CODE', 'R_NAME', 'R_ADDRESS', 'KHBH_DQ'], axis=1, inplace=False),
                                how='inner', on=['BUSI_DATE','PROV_CODE','CITY_CODE','R_KHTYBM'])
            self.logger.info(" valid_df=%s",len(valid_df))
            self.logger.info(" valid_df=%s",valid_df.columns)
            valid_df = valid_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'],
                                                keep='first',inplace=False)
            self.logger.info(" valid_df=%s",len(valid_df))
            # 3.0插入数据库
            PRE_SQL = "INSERT INTO RFM_CORP_RTL_VALID(BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, PROV_CODE," \
                      " PROV_NAME, CITY_CODE,CITY_NAME,R_KHTYBM,R_CODE,R_XKZH, R_NAME,R_ADDRESS," + \
                      " VISIT_CODE,VISIT_TYPE,VISIT_NAME,R_JYYT,R_CXFL," + \
                      " R_DW,R_CONTACTOR,R_TEL,R_NUM,R_RANK, F_NUM,F_RANK,M_NUM,M_RANK,X_NUM,X_RANK,LSH_GGS,ZJE," + \
                      " ZJE_RANK,ZDX,ZDX_RANK,Z_RANK,Z_SCORE) VALUES "
            i_sql = ""
            count = 0
            values = {'R_XKZH': '', 'R_NAME': '', 'R_ADDRESS': '','VISIT_CODE': '','VISIT_TYPE': '',
                      'R_JYYT': '', 'R_CXFL': '', 'R_DW': '', 'R_CONTACTOR': '', 'R_TEL': ''}
            valid_df.fillna(value=values, inplace=True)
            valid_df.fillna(0, inplace=True)
            pre_city_code = ""
            city_cv_df = None
            for row in valid_df.index:
                busi_date = valid_df.loc[row,'BUSI_DATE']
                prov_code = valid_df.loc[row,'PROV_CODE']
                pro_name = valid_df.loc[row,'PROV_NAME']
                city_code = valid_df.loc[row,'CITY_CODE']
                city_name = valid_df.loc[row,'CITY_NAME']
                r_khtym = valid_df.loc[row,'R_KHTYBM']
                r_code = valid_df.loc[row,'R_CODE']

                z_rank = -1
                z_score = 0.0
                if pre_city_code == city_code:
                    pre_city_code = city_code
                else:
                    city_cv_df = cv_df[cv_df['CITY_CODE']==city_code]
                    city_cv_df.sort_values(by=['Z_SCORE'], ascending=False, inplace=True)
                    city_cv_df = city_cv_df.reset_index(drop = True)
                    pre_city_code = city_code
                pass
                try:
                    z_score = city_cv_df[(city_cv_df['CITY_CODE']==city_code) &
                                         (city_cv_df['R_KHTYBM']==r_khtym)]['Z_SCORE'].values[0]
                    z_rank = city_cv_df[(city_cv_df['CITY_CODE']==city_code) &
                                        (city_cv_df['R_KHTYBM']==r_khtym)].index.values[0]
                except Exception as ex1:
                    self.logger.error(" city_cv_df:\n%s",str(ex1))
                pass
                z_score = str(z_score)
                if z_rank >= 0:
                    z_rank = z_rank + 1
                pass
                z_rank = str(z_rank)
                r_xkzh = valid_df.loc[row,'R_XKZH']
                r_name = valid_df.loc[row,'R_NAME']
                r_add = valid_df.loc[row,'R_ADDRESS']
                r_visit_code = valid_df.loc[row,'VISIT_CODE']
                r_visit_type = valid_df.loc[row,'VISIT_TYPE']
                r_visit_name = valid_df.loc[row,'VISIT_NAME']
                r_jyty = valid_df.loc[row,'R_JYYT']
                r_cxfl = valid_df.loc[row,'R_CXFL']
                r_dw = valid_df.loc[row,'R_DW']
                r_contactor = valid_df.loc[row,'R_CONTACTOR']
                r_tel = valid_df.loc[row,'R_TEL']
                r_num = str(valid_df.loc[row,'R_NUM'])
                r_rank = str(valid_df.loc[row,'R_RANK'])
                r_f_num = str(valid_df.loc[row,'F_NUM'])
                r_f_rank = str(valid_df.loc[row,'F_RANK'])
                r_m_num = str(valid_df.loc[row,'M_NUM'])
                r_m_rank = str(valid_df.loc[row,'M_RANK'])
                r_x_num = str(valid_df.loc[row,'X_NUM'])
                r_x_rank = str(valid_df.loc[row,'X_RANK'])
                r_lsh_ggs = str(valid_df.loc[row,'LSH_GGS'])
                r_zje = str(valid_df.loc[row,'ZJE'])
                r_zje_rank = str(valid_df.loc[row,'ZJE_RANK'])
                r_zdx = str(valid_df.loc[row,'ZDX'])
                r_zdx_rank = str(valid_df.loc[row,'ZDX_RANK'])

                i_sql = i_sql + " ('" + busi_date + "', '" + corp_code + "', '" + corp_name + "', '" \
                        + group_code + "', '" + prov_code + "', '" + pro_name + "', '" \
                        + city_code + "', '" + city_name + "', '" + r_khtym + "','"  + r_code + "', '" \
                        + r_xkzh + "', '" + r_name + "', '" + r_add + "','" + r_visit_code + "','" \
                        + r_visit_type + "','"   + r_visit_name + "','"   + r_jyty + "', '" \
                        + r_cxfl + "', '" + r_dw + "', '" + r_contactor + "', '" + r_tel + "',"  + r_num + ", " \
                        + r_rank + "," + r_f_num + "," + r_f_rank + ","  + r_m_num + "," \
                        + r_m_rank + "," + r_x_num + "," + r_x_rank + "," + r_lsh_ggs + "," + r_zje + "," \
                        + r_zje_rank + "," + r_zdx + "," + r_zdx_rank + "," + z_rank + "," + z_score + "),"
                count = count + 1
                if count%2000 == 0:
                    i_sql = i_sql.rstrip(',')
                    i_sql = PRE_SQL + i_sql
                    results = db.insert(i_sql)
                    i_sql = ""
                pass
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info(" 成功插入数据库记录数 = :%s",count)

            # 4.0 无效零售户处理
            # 4.1 无匹配零售户
            s_sql = "SELECT BUSI_DATE, PROV_CODE, CITY_CODE, R_CODE, R_XKZH, R_NAME, R_ADDRESS " + \
                    " FROM  " + tbl_2 + " WHERE  BUSI_DATE='" + self.busidate + "' " + \
                    " order by PROV_CODE, CITY_CODE"
            zyh_rec = db.select(s_sql)
            self.logger.info(" s_sql=:\n%s",s_sql)
            zyh_df = pd.DataFrame(list(zyh_rec), columns=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE',
                                                          'R_XKZH','R_NAME', 'R_ADDRESS'])

            self.logger.info(" zyh_df=%s",len(zyh_df))
            zyh_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'], inplace=True)
            self.logger.info(" zyh_df=%s",len(zyh_df))

            # 当月自有户在核心户中不存在的为无效户
            invalid_df = zyh_df.append(zy_df.drop(['R_KHTYBM','VISIT_CODE', 'VISIT_TYPE'], axis=1, inplace=False))
            invalid_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'],keep=False, inplace=True)
            self.logger.info(" invalid_df=%s",len(invalid_df))

            # 插入数据库
            PRE_SQL = "INSERT INTO RFM_CORP_RTL_ERR (BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, PROV_CODE, " \
                      " CITY_CODE,R_CODE,R_TYPE) VALUES "
            i_sql = ""
            count = 0
            for row in invalid_df.index:
                busi_date = invalid_df.loc[row,'BUSI_DATE']
                prov_code = invalid_df.loc[row,'PROV_CODE']
                city_code = invalid_df.loc[row,'CITY_CODE']
                r_code = invalid_df.loc[row,'R_CODE']
                i_sql = i_sql + " ('" + busi_date + "', '" + corp_code + "', '" + corp_name + "', '" \
                        + group_code + "', '" + prov_code + "', '" + city_code + "', '"  + r_code + "', '0'),"
                count = count + 1
                if count%2000 == 0:
                    i_sql = i_sql.rstrip(',')
                    i_sql = PRE_SQL + i_sql
                    results = db.insert(i_sql)
                    i_sql = ""
                pass
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info("无匹配零售户处理成功记录数 = :%s",count)

            # 4.2 无销量零售户
            nosale_df_1 = zy_df[['BUSI_DATE', 'PROV_CODE','CITY_CODE','R_CODE']]
            valid_df_1 = valid_df[['BUSI_DATE', 'PROV_CODE','CITY_CODE','R_CODE']]
            # 插入数据库
            # 取差
            invalid_df = nosale_df_1.append(valid_df_1)
            invalid_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'],keep=False, inplace=True)
            i_sql = ""
            count = 0
            for row in invalid_df.index:
                busi_date = invalid_df.loc[row,'BUSI_DATE']
                prov_code = invalid_df.loc[row,'PROV_CODE']
                city_code = invalid_df.loc[row,'CITY_CODE']
                r_code = invalid_df.loc[row,'R_CODE']
                i_sql = i_sql + " ('" + busi_date + "', '" + corp_code + "', '" + corp_name + "', '" \
                        + group_code + "', '" + prov_code + "', '" + city_code + "', '"  + r_code + "', '1'),"
                count = count + 1
                if count%2000 == 0:
                    i_sql = i_sql.rstrip(',')
                    i_sql = PRE_SQL + i_sql
                    results = db.insert(i_sql)
                    i_sql = ""
                pass
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info("无销量零售户处理成功记录数 = :%s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql=%s",i_sql)
        finally:
            db.close()
        pass
    pass
    ##########################################
    # 各中烟未达标核心客户信息筛选
    ##########################################
    def doUnQulityRtl(self, corp_code, corp_name, group_code):
        # 信息匹配
        db = MysqlDB(logger=self.logger)
        i_sql = ""
        try:
            db.connect()
            # 1.0 插入前先删除未达标的零售户信息表
            # 删除未达标的零售户信息表
            d_sql = "delete from RFM_CORP_RTL_UNQUALIFY where BUSI_DATE = '" + self.busidate + "' " + \
                    " and CORP_CODE = '" + corp_code + "' and GROUP_CODE = '" + group_code + "'"
            ret = db.delete(d_sql)
            self.logger.info(" 未达标的零售户信息表删除成功= %s", corp_name + " - " + self.busidate + "-" + str(ret))
            # 2.0插入数据库
            i_sql = "INSERT INTO RFM_CORP_RTL_UNQUALIFY (BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, PROV_CODE," \
                    " PROV_NAME, CITY_CODE,CITY_NAME,R_KHTYBM,R_CODE,R_XKZH, R_NAME,R_ADDRESS," + \
                    " VISIT_CODE,VISIT_TYPE,R_JYYT,R_CXFL," + \
                    " R_DW,R_CONTACTOR,R_TEL,R_NUM,R_RANK, F_NUM,F_RANK,M_NUM,M_RANK,X_NUM,X_RANK,LSH_GGS,ZJE," + \
                    " ZJE_RANK,ZDX,ZDX_RANK) " + \
                    " SELECT distinct a.BUSI_DATE, a.CORP_CODE, a.CORP_NAME, a.GROUP_CODE, a.PROV_CODE," + \
                    " a.PROV_NAME, a.CITY_CODE, a.CITY_NAME, a.R_KHTYBM, a.R_CODE, a.R_XKZH, a. R_NAME, a.R_ADDRESS," + \
                    " a.VISIT_CODE, a.VISIT_TYPE, a.R_JYYT, a.R_CXFL," + \
                    " a.R_DW, a.R_CONTACTOR, a.R_TEL, a.R_NUM, a.R_RANK, a.F_NUM,a.F_RANK,a.M_NUM,a.M_RANK,a.X_NUM," + \
                    " a.X_RANK,a.LSH_GGS,a.ZJE, a.ZJE_RANK, a.ZDX, a.ZDX_RANK " + \
                    " from RFM_CORP_RTL_VALID a, RFM_RTL_FEATURE_CITY b " + \
                    " where a.CITY_CODE = b.CITY_CODE and a.BUSI_DATE = b.BUSI_DATE and a.CORP_CODE = b.CORP_CODE " + \
                    " and a.GROUP_CODE = b.GROUP_CODE and a.BUSI_DATE = '" + self.busidate + "' " + \
                    " and a.CORP_CODE = '" + corp_code + "' and a.GROUP_CODE = '" + group_code + "' " + \
                    " and a.LSH_GGS > 0 and (a.X_RANK > (b.ZY_RTL_NUM * 0.4) or (a.F_NUM/a.LSH_GGS/3.0) < 1.5) "
            self.logger.info(" i_sql = :%s",i_sql)
            ret = db.insert(i_sql)
            self.logger.info(" 成功插入未达标的零售户信息表记录数 = :%s",ret)

            # 3.0 删除推荐客户中的未达标的零售户
            d_sql = "delete t1 from RFM_VALUE_RTL_RECOM t1, RFM_CORP_RTL_UNQUALIFY t2 " + \
                    " where t1.BUSI_DATE = t2.BUSI_DATE and t1.CORP_CODE = t2.CORP_CODE " + \
                    " and t1.GROUP_CODE = t2.GROUP_CODE and t1.PROV_CODE = t2.PROV_CODE " + \
                    " and t1.CITY_CODE = t2.CITY_CODE and t1.R_KHTYBM = t2.R_KHTYBM " + \
                    " and t1.BUSI_DATE = '" + self.busidate + "' and t1.CORP_CODE = '" + corp_code + "' " + \
                    " and t1.GROUP_CODE = '" + group_code + "' "
            self.logger.info(" d_sql = :%s",d_sql)
            ret = db.delete(d_sql)
            self.logger.info(" 从价值客户推荐表中删除未达标的零售户成功= %s", corp_name + " - " + self.busidate + "-" + str(ret))
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql=%s",i_sql)
        finally:
            db.close()
        pass
    pass
    ##########################################
    # 处理无效零售户
    ##########################################
    def processErrRtl(self, corp_code, corp_name, group_code):
        # 无效零售户处理 -- 3个月内无销量
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            # 1.0 插入前先删除有效核心零售户信息表和无效核心零售户信息表
            # 删除有效核心零售户信息表
            d_sql = "delete from RFM_CORP_RTL_CAT_ERR where BUSI_DATE = '" + self.busidate + "' " + \
                    " and CORP_CODE = '" + corp_code + "' and GROUP_CODE = '" + group_code + "'"
            db.delete(d_sql)
            self.logger.info(" 各中烟无效的零售户分类表删除成功= \n%s", corp_name + " - " + self.busidate)

            # 2.经过匹配后，各中烟无效的零售户
            s_sql = "SELECT BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, PROV_CODE, CITY_CODE, trim(R_CODE) R_CODE " + \
                    " FROM  RFM_CORP_RTL_ERR WHERE  BUSI_DATE='" + self.busidate + "'  and CORP_CODE='" + corp_code +"' " + \
                    " and GROUP_CODE = '" + group_code + "' order by PROV_CODE, CITY_CODE "
            rtl_rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            invalid_df = pd.DataFrame(list(rtl_rec), columns=['BUSI_DATE', 'CORP_CODE', 'CORP_NAME', 'GROUP_CODE',
                                                              'PROV_CODE','CITY_CODE', 'R_CODE'])


            codes_df = invalid_df[['PROV_CODE','CITY_CODE']]
            city_codes = codes_df.drop_duplicates(subset=['PROV_CODE','CITY_CODE'], inplace=False)
            citys = city_codes['CITY_CODE'].values
            citys = "('" + "','".join(citys) + "')"
            # 从大数据平台获取数据
            all_rtl = self.getAllRtlInfoFromHive(citys)

            match_rtl = pd.merge(invalid_df, all_rtl.drop(['R_NAME', 'R_ADDRESS'], axis=1, inplace=False),
                                 how='inner', on=['CITY_CODE','R_CODE'])
            self.logger.info(" match_rtl=%s",len(match_rtl))
            self.logger.info(" match_rtl=%s",match_rtl.columns)
            match_rtl.drop_duplicates(subset=['CITY_CODE','R_CODE'], inplace=True)
            self.logger.info(" match_rtl=%s",len(match_rtl))
            self.logger.info(" match_rtl=%s",match_rtl.columns)

            # 插入数据库
            PRE_SQL = "INSERT INTO RFM_CORP_RTL_CAT_ERR (BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, PROV_CODE, " \
                      " CITY_CODE,R_CODE, R_TYPE) VALUES "
            i_sql = ""
            count = 0
            for row in match_rtl.index:
                busi_date = match_rtl.loc[row,'BUSI_DATE']
                prov_code = match_rtl.loc[row,'PROV_CODE']
                # pro_name = match_rtl.loc[row,'PROV_NAME']
                city_code = match_rtl.loc[row,'CITY_CODE']
                r_code = match_rtl.loc[row,'R_CODE']
                i_sql = i_sql + " ('" + busi_date + "', '" + corp_code + "', '" + corp_name + "', '" \
                        + group_code + "', '" + prov_code + "', '" + city_code + "', '"  + r_code + "', '1'),"
                count = count + 1
                if count%100 == 0:
                    i_sql = i_sql.rstrip(',')
                    i_sql = PRE_SQL + i_sql
                    results = db.insert(i_sql)
                    i_sql = ""
                pass
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info("无销量零售户处理成功记录数 = :%s",count)
            # 取无效户
            invalid_df = invalid_df.append(match_rtl)
            invalid_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'R_CODE'],keep=False, inplace=True)
            i_sql = ""
            count = 0
            for row in invalid_df.index:
                busi_date = invalid_df.loc[row,'BUSI_DATE']
                prov_code = invalid_df.loc[row,'PROV_CODE']
                # pro_name = invalid_df.loc[row,'PROV_NAME']
                city_code = invalid_df.loc[row,'CITY_CODE']
                r_code = invalid_df.loc[row,'R_CODE']
                i_sql = i_sql + " ('" + busi_date + "', '" + corp_code + "', '" + corp_name + "', '" \
                        + group_code + "', '" + prov_code + "', '" + city_code + "', '"  + r_code + "', '0'),"
                count = count + 1
                if count%100 == 0:
                    i_sql = i_sql.rstrip(',')
                    i_sql = PRE_SQL + i_sql
                    results = db.insert(i_sql)
                    i_sql = ""
                pass
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info("无匹配零售户处理成功记录数 = :%s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
        finally:
            db.close()
        pass
    pass
    ##########################################
    # 各中烟价值零售户价值特征表(分地市)
    ##########################################
    def doCityFeature(self, corp_code, corp_name, group_code, tbl):
        # 0.获取数据
        rst_df = self.getCityFeatureFromHive(self.busidate,tbl)

        # 1.零售户价值特征表(分地市)
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            # 2.0 查配置表
            # 2.0 插入前先删除零售户价值特征表(分地市)
            d_sql = "delete from RFM_RTL_FEATURE_CITY where BUSI_DATE = '" + self.busidate + "' " \
                                                                                             " and CORP_CODE = '" + corp_code + "' and GROUP_CODE = '" + group_code + "'"
            ret = db.delete(d_sql)
            self.logger.info(" 零售户价值特征表(分地市)表删除成功= \n%s", corp_name + " - " + self.busidate + "-" + str(ret))
            PRE_SQL = "INSERT INTO RFM_RTL_FEATURE_CITY(BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, PROV_CODE," \
                      " PROV_NAME, CITY_CODE,CITY_NAME,ZY_RTL_NUM,F_NUM,M_NUM,X_NUM,HJGJCS,ALL_RTL_NUM,ZJE,ZXL) VALUES "
            i_sql = ""
            count = 0
            for row in rst_df.index:
                prov_code = rst_df.loc[row,'PROV_CODE']
                pro_name = rst_df.loc[row,'PROV_NAME']
                city_code = rst_df.loc[row,'CITY_CODE']
                city_name = rst_df.loc[row,'CITY_NAME']
                r_zy_rtl_num = str(rst_df.loc[row,'ZY_RTL_NUM'])
                r_f_num = str(rst_df.loc[row,'F_NUM'])
                r_m_num = str(rst_df.loc[row,'M_NUM'])
                r_x_num = str(rst_df.loc[row,'X_NUM'])
                r_hjgjcs = str(rst_df.loc[row,'HJGJCS'])
                r_all_rtl_num = str(rst_df.loc[row,'ALL_RTL_NUM'])
                r_zje = str(rst_df.loc[row,'ZJE'])
                r_zxl = str(rst_df.loc[row,'ZXL'])

                i_sql = i_sql + " ('" + self.busidate + "', '" + corp_code + "', '" + corp_name + "', '" \
                        + group_code + "', '" + prov_code + "', '" + pro_name + "', '" \
                        + city_code + "', '" + city_name + "', " + r_zy_rtl_num + ", " \
                        + r_f_num + "," +  r_m_num + "," +  r_x_num + "," + r_hjgjcs + "," + r_all_rtl_num + "," \
                        + r_zje + "," + r_zxl + "),"
                count = count + 1
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info(" 零售户价值特征表(分地市)成功记录数:\n%s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
        finally:
            db.close()
        pass
    pass
    ##########################################
    # 各中烟拜访人的核心户绩效评价分析(分地市)
    ##########################################
    def doVisitorRevalue(self, corp_code, corp_name, group_code, busi_date, tbl2):

        db = MysqlDB(logger=self.logger)
        i_sql = ""
        try:
            db.connect()
            # 1.获取数据
            # 1.1 拜访人的核心户
            s_sql = "SELECT a.BUSI_DATE, a.PROV_CODE, a.PROV_NAME, a.CITY_CODE, a.CITY_NAME, a.VISIT_CODE, a.VISIT_NAME,a.VISIT_TYPE," + \
                    " (avg(b.F_NUM/b.LSH_GGS) / 3.0) BFR_HJCS, (avg(b.X_NUM) / 3.0) BFR_HJXL, " + \
                    " (sum(b.M_NUM)/sum(b.X_NUM) /40.0) BFR_DX FROM " + tbl2 +" a left join RFM_CORP_RTL_VALID b " + \
                    " on a.BUSI_DATE=b.BUSI_DATE and a.PROV_CODE=b.PROV_CODE and a.CITY_CODE=b.CITY_CODE and " + \
                    " a.R_CODE=b.R_CODE and b.CORP_CODE='" + corp_code + "' and  b.GROUP_CODE='" + group_code + "' " + \
                    " WHERE  a.BUSI_DATE='" + busi_date + "' " + \
                    " group by a.BUSI_DATE,a.PROV_CODE,a.PROV_NAME,a.CITY_CODE,a.CITY_NAME,a.VISIT_CODE,a.VISIT_NAME, a.VISIT_TYPE "
            rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            bfr_df = pd.DataFrame(list(rec), columns=['BUSI_DATE','PROV_CODE','PROV_NAME','CITY_CODE','CITY_NAME',
                                                      'VISIT_CODE','VISIT_NAME','VISIT_TYPE','BFR_HJCS','BFR_HJXL','BFR_DX'])
            bfr_df.drop_duplicates(subset=['BUSI_DATE', 'PROV_CODE', 'CITY_CODE', 'VISIT_CODE'], inplace=True)
            bfr_df.fillna(0.0, inplace=True)
            # 1.2 核心户
            s_sql = "SELECT BUSI_DATE, PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME," + \
                    " (avg(F_NUM/LSH_GGS) / 3.0) HXH_HJCS, (avg(X_NUM) / 3.0) HXH_HJXL, " + \
                    " (sum(M_NUM)/sum(X_NUM) /40.0) HXH_DX FROM RFM_CORP_RTL_VALID " + \
                    " WHERE BUSI_DATE='" + busi_date + "' and CORP_CODE='" + corp_code + "' and " + \
                    " GROUP_CODE='" + group_code + "' " + \
                    " group by BUSI_DATE, PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME "
            rec2 = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            hxh_df = pd.DataFrame(list(rec2), columns=['BUSI_DATE','PROV_CODE','PROV_NAME','CITY_CODE','CITY_NAME',
                                                       'HXH_HJCS','HXH_HJXL','HXH_DX'])
            hxh_df.fillna(0.0, inplace=True)

            # 1.3 自有零售户
            s_sql = "SELECT BUSI_DATE, PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME," + \
                    " HJGJCS ZYH_HJCS, (X_NUM / ZY_RTL_NUM / 3.0) ZYH_HJXL, " + \
                    " (M_NUM/X_NUM/40.0) ZYH_DX FROM RFM_RTL_FEATURE_CITY " + \
                    " WHERE BUSI_DATE='" + busi_date + "' and CORP_CODE='" + corp_code + "' and " + \
                    " GROUP_CODE='" + group_code + "'  "
            rec3 = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            zyh_df = pd.DataFrame(list(rec3), columns=['BUSI_DATE','PROV_CODE','PROV_NAME','CITY_CODE','CITY_NAME',
                                                       'ZYH_HJCS','ZYH_HJXL','ZYH_DX'])
            zyh_df.fillna(0.0, inplace=True)

            # 2.0 插入前先删除零售户价值特征表(分地市)
            d_sql = "delete from RFM_VISITOR_REVALUE_CITY where BUSI_DATE = '" + self.busidate + "' " + \
                    " and CORP_CODE = '" + corp_code + "' and GROUP_CODE = '" + group_code + "'"
            ret = db.delete(d_sql)
            self.logger.info(" 拜访人绩效表(分地市)表(RFM_VISITOR_REVALUE_CITY)删除成功= \n%s",
                             corp_name + " - " + self.busidate + "-" + str(ret))
            PRE_SQL = "INSERT INTO RFM_VISITOR_REVALUE_CITY(BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, PROV_CODE," \
                      " PROV_NAME, CITY_CODE,CITY_NAME,VISIT_CODE,VISIT_NAME,VISIT_TYPE,BFR_HJCS,BFR_HJXL,BFR_DX," + \
                      " HXH_HJCS,HXH_HJXL,HXH_DX,ZYH_HJCS,ZYH_HJXL,ZYH_DX) VALUES "
            count = 0
            for row in bfr_df.index:
                prov_code = bfr_df.loc[row,'PROV_CODE']
                pro_name = bfr_df.loc[row,'PROV_NAME']
                city_code = bfr_df.loc[row,'CITY_CODE']
                city_name = bfr_df.loc[row,'CITY_NAME']
                visit_code = bfr_df.loc[row,'VISIT_CODE']
                visit_name = bfr_df.loc[row,'VISIT_NAME']
                visit_type = bfr_df.loc[row,'VISIT_TYPE']
                bfr_hjcs = str(bfr_df.loc[row,'BFR_HJCS'])
                bfr_hjxl = str(bfr_df.loc[row,'BFR_HJXL'])
                bfr_dx = str(bfr_df.loc[row,'BFR_DX'])

                h_df = hxh_df[(hxh_df['PROV_CODE']==prov_code) & (hxh_df['CITY_CODE']==city_code)]
                if h_df is None or len(h_df) ==0:
                    hxh_hjcs = '0.0'
                    hxh_hjxl = '0.0'
                    hxh_dx = '0.0'
                else:
                    hxh_hjcs = str(h_df['HXH_HJCS'].values[0])
                    hxh_hjxl = str(h_df['HXH_HJXL'].values[0])
                    hxh_dx = str(h_df['HXH_DX'].values[0])
                pass

                z_df = zyh_df[(zyh_df['PROV_CODE']==prov_code) & (zyh_df['CITY_CODE']==city_code)]
                if z_df is None or len(z_df) ==0:
                    zyh_hjcs = '0.0'
                    zyh_hjxl = '0.0'
                    zyh_dx = '0.0'
                else:
                    zyh_hjcs = str(z_df['ZYH_HJCS'].values[0])
                    zyh_hjxl = str(z_df['ZYH_HJXL'].values[0])
                    zyh_dx = str(z_df['ZYH_DX'].values[0])
                pass

                i_sql = i_sql + " ('" + self.busidate + "', '" + corp_code + "', '" + corp_name + "', '" + \
                        group_code + "', '" + prov_code + "', '" + pro_name + "', '" + \
                        city_code + "', '" + city_name + "','" + visit_code + "','" + visit_name + "', '" + visit_type + "', " + \
                        bfr_hjcs + "," + bfr_hjxl + "," + bfr_dx + "," + hxh_hjcs + "," + hxh_hjxl + "," + \
                        hxh_dx + "," +  zyh_hjcs + "," +  zyh_hjxl + "," + zyh_dx + "),"
                count = count + 1
            pass
            if (i_sql != ""):
                i_sql = i_sql.rstrip(',')
                i_sql = PRE_SQL + i_sql
                ret = db.insert(i_sql)
            pass
            self.logger.info(" 拜访人绩效表(分地市)表(RFM_VISITOR_REVALUE_CITY)成功记录数=%s",count)
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",i_sql)
        finally:
            db.close()
        pass
    pass
    ##########################################
    # 各中烟推荐价值客户匹配(分地市)
    ##########################################
    def doRecomRtlMatch(self, corp_code, corp_name, group_code, busi_date, tbl2):

        db = MysqlDB(logger=self.logger)
        i_sql = ""
        try:
            db.connect()
            # 1.获取数据
            # 1.1 总零售户数、自有零售户数量
            s_sql = "SELECT BUSI_DATE, PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME, ALL_RTL_NUM, ZY_RTL_NUM " + \
                    " FROM RFM_RTL_FEATURE_CITY WHERE  BUSI_DATE='" + busi_date + "' and CORP_CODE='" + corp_code + "' " + \
                    " and GROUP_CODE='" + group_code + "' order by PROV_CODE, CITY_CODE "
            rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            all_df = pd.DataFrame(list(rec), columns=['BUSI_DATE','PROV_CODE','PROV_NAME','CITY_CODE','CITY_NAME',
                                                      'ALL_RTL_NUM','ZY_RTL_NUM'])
            all_df.fillna(0.0, inplace=True)
            self.logger.info(" all_df=%s",all_df.columns)
            self.logger.info(" all_df=%s",len(all_df))
            # 1.2 核心户
            s_sql = "SELECT distinct CITY_CODE, R_KHTYBM FROM RFM_CORP_RTL_VALID " + \
                    " WHERE BUSI_DATE='" + busi_date + "' and CORP_CODE='" + corp_code + "' and " + \
                    " GROUP_CODE='" + group_code + "' " + \
                    " order by CITY_CODE "
            rec2 = db.select(s_sql)
            # self.logger.info(" SQL=:\n%s",s_sql)
            hxh_df = pd.DataFrame(list(rec2), columns=['CITY_CODE', 'R_KHTYBM'])
            hxh_df.fillna(0.0, inplace=True)
            hxh_group_df=hxh_df.groupby(hxh_df["CITY_CODE"]).agg("count")
            new_hxh_df = pd.DataFrame(columns=['CITY_CODE', 'HXH_RTL_NUM'])
            count = 0
            for row in hxh_group_df.index:
                new_hxh_df.loc[count]= [row,hxh_group_df.loc[row,'R_KHTYBM']]
                count += 1
            pass
            self.logger.info(" new_hxh_df=%s",new_hxh_df.columns)
            self.logger.info(" new_hxh_df=%s",len(new_hxh_df))
            # 1.2.1 内部核心户/外部核心户
            s_sql = "SELECT CITY_CODE, R_KHTYBM, R_CODE  FROM RFM_CORP_RTL_VALID " + \
                    " WHERE BUSI_DATE='" + busi_date + "' and CORP_CODE='" + corp_code + "' and " + \
                    " GROUP_CODE='" + group_code + "' " + \
                    " order by CITY_CODE "
            rec2 = db.select(s_sql)
            # self.logger.info(" SQL=:\n%s",s_sql)
            hxh_valid_df = pd.DataFrame(list(rec2), columns=['CITY_CODE', 'R_KHTYBM', 'R_CODE'])
            # self.logger.info(" hxh_valid_df=%s",hxh_valid_df.columns)
            # self.logger.info(" hxh_valid_df=%s",len(hxh_valid_df))

            s_sql = "SELECT CITY_CODE, R_CODE, VISIT_TYPE FROM " + tbl2  + \
                    " WHERE BUSI_DATE='" + busi_date + "'  order by CITY_CODE "
            rec2 = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            hxh_zy_df = pd.DataFrame(list(rec2), columns=['CITY_CODE', 'R_CODE','VISIT_TYPE'])
            # self.logger.info(" hxh_zy_df=%s",hxh_zy_df.columns)
            # self.logger.info(" hxh_zy_df=%s",len(hxh_zy_df))

            hxh_io_df = pd.merge(hxh_valid_df, hxh_zy_df, how='inner', on=['CITY_CODE','R_CODE'])
            self.logger.info(" hxh_io_df=%s",hxh_io_df.columns)
            self.logger.info(" hxh_io_df=%s",len(hxh_io_df))
            hxh_io_df.drop(['R_CODE'], axis=1, inplace=True)
            hxh_io_df.fillna(0.0, inplace=True)

            hxh_inner_df = hxh_io_df[hxh_io_df['VISIT_TYPE'] == '0'].drop(['VISIT_TYPE'], axis=1, inplace=False)
            hxh_outer_df = hxh_io_df[hxh_io_df['VISIT_TYPE'] == '1'].drop(['VISIT_TYPE'], axis=1, inplace=False)
            hxh_inner_group_df=hxh_inner_df.groupby(hxh_inner_df["CITY_CODE"]).agg("count")
            hxh_outer_group_df=hxh_outer_df.groupby(hxh_outer_df["CITY_CODE"]).agg("count")
            new_inner_hxh_df = pd.DataFrame(columns=['CITY_CODE', 'HXH_RTL_INNER_NUM'])
            count = 0
            for row in hxh_inner_group_df.index:
                new_inner_hxh_df.loc[count]= [row,hxh_inner_group_df.loc[row,'R_KHTYBM']]
                count += 1
            pass
            new_outer_hxh_df = pd.DataFrame(columns=['CITY_CODE', 'HXH_RTL_OUTER_NUM'])
            count = 0
            for row in hxh_outer_group_df.index:
                new_outer_hxh_df.loc[count]= [row, hxh_outer_group_df.loc[row,'R_KHTYBM']]
                count += 1
            pass

            # 1.3 推荐零售户
            s_sql = "SELECT CITY_CODE, R_KHTYBM FROM RFM_VALUE_RTL_RECOM " + \
                    " WHERE BUSI_DATE='" + busi_date + "' and CORP_CODE='" + corp_code + "' and " + \
                    " GROUP_CODE='" + group_code + "' " + \
                    " order by CITY_CODE, Z_SCORE desc "
            rec2 = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            all_recom_df = pd.DataFrame(list(rec2), columns=['CITY_CODE', 'R_KHTYBM'])
            all_recom_df.fillna(0.0, inplace=True)
            self.logger.info(" all_recom_df=%s",all_recom_df.columns)
            self.logger.info(" all_recom_df=%s",len(all_recom_df))

            # 1.4 计算实际拜访的总户数
            s_sql = "SELECT CITY_CODE, count(distinct R_KHTYBM) BF_NUM " + \
                    " FROM RFM_CORP_RTL_VALID " + \
                    " WHERE BUSI_DATE='" + self.busidate + "' and CORP_CODE='" + corp_code + "' and " + \
                    " GROUP_CODE='" + group_code + "' " + \
                    " group by  CITY_CODE "
            rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            bfzs_df = pd.DataFrame(list(rec), columns=['CITY_CODE','BF_NUM'])
            bfzs_df.fillna(0.0, inplace=True)
            self.logger.info(" bfzs_df=%s",bfzs_df.columns)
            self.logger.info(" bfzs_df=%s",len(bfzs_df))

            # 按不同比例加工处理
            for ratio in self.RATIOS:
                recom_df = None
                recom_df = pd.DataFrame(columns=['CITY_CODE', 'R_KHTYBM'])
                for row in bfzs_df.index:
                    num = bfzs_df.loc[row,'BF_NUM']
                    city_code = bfzs_df.loc[row,'CITY_CODE']
                    std_num = int(num * ratio) + 1
                    recom_df = recom_df.append(all_recom_df[all_recom_df['CITY_CODE'] == city_code][0:std_num])
                pass

                recom_group_df=recom_df.groupby(recom_df["CITY_CODE"]).agg("count")
                new_recom_df = pd.DataFrame(columns=['CITY_CODE', 'RECOM_RTL_NUM'])
                count = 0
                for row in recom_group_df.index:
                    new_recom_df.loc[count]= [row,recom_group_df.loc[row,'R_KHTYBM']]
                    count += 1
                pass

                # 1.4 推荐匹配
                match_df = pd.merge(hxh_df, recom_df, how='inner', on=['CITY_CODE','R_KHTYBM'])
                match_group_df=match_df.groupby(match_df["CITY_CODE"]).agg("count")
                new_match_df = pd.DataFrame(columns=['CITY_CODE', 'MATCH_RTL_NUM'])
                count = 0
                for row in match_group_df.index:
                    new_match_df.loc[count]= [row,match_group_df.loc[row,'R_KHTYBM']]
                    count += 1
                pass
                self.logger.info(" new_match_df=%s",new_match_df.columns)
                self.logger.info(" new_match_df=%s",len(new_match_df))

                # 1.4.1 推荐匹配:内部零售户
                match_io_df = pd.merge(hxh_io_df, recom_df, how='inner', on=['CITY_CODE','R_KHTYBM'])
                match_inner_df = match_io_df[match_io_df['VISIT_TYPE'] == '0'].drop(['VISIT_TYPE'], axis=1, inplace=False)
                match_outer_df = match_io_df[match_io_df['VISIT_TYPE'] == '1'].drop(['VISIT_TYPE'], axis=1, inplace=False)
                match_inner_group_df=match_inner_df.groupby(match_inner_df["CITY_CODE"]).agg("count")
                match_outer_group_df=match_outer_df.groupby(match_outer_df["CITY_CODE"]).agg("count")
                new_inner_match_df = pd.DataFrame(columns=['CITY_CODE', 'MATCH_RTL_INNER_NUM'])
                count = 0
                for row in match_inner_group_df.index:
                    new_inner_match_df.loc[count]= [row,match_inner_group_df.loc[row,'R_KHTYBM']]
                    count += 1
                pass
                new_outer_match_df = pd.DataFrame(columns=['CITY_CODE', 'MATCH_RTL_OUTER_NUM'])
                count = 0
                for row in match_outer_group_df.index:
                    new_outer_match_df.loc[count]= [row,match_outer_group_df.loc[row,'R_KHTYBM']]
                    count += 1
                pass

                # 1.5 组合数据
                zh_df = pd.merge(all_df, new_hxh_df, how='left', on=['CITY_CODE'])
                zh_df = pd.merge(zh_df, new_inner_hxh_df, how='left', on=['CITY_CODE'])
                zh_df = pd.merge(zh_df, new_outer_hxh_df, how='left', on=['CITY_CODE'])
                zh_df = pd.merge(zh_df, new_recom_df, how='left', on=['CITY_CODE'])
                zh_df = pd.merge(zh_df, new_match_df, how='left', on=['CITY_CODE'])
                zh_df = pd.merge(zh_df, new_inner_match_df, how='left', on=['CITY_CODE'])
                zh_df = pd.merge(zh_df, new_outer_match_df, how='left', on=['CITY_CODE'])
                zh_df.fillna(0.0, inplace=True)
                self.logger.info(" zh_df=%s",zh_df.columns)
                self.logger.info(" zh_df=%s",len(zh_df))

                # 2.0 插入前先删除推荐价值客户匹配表(分地市)
                d_sql = "delete from RFM_RTL_RECOM_MATCH where BUSI_DATE = '" + self.busidate + "' " + \
                        " and CORP_CODE = '" + corp_code + "' and GROUP_CODE = '" + group_code + "' and " +\
                        " TIMES_TYPE = '" + str(ratio) + "' "
                ret = db.delete(d_sql)
                self.logger.info(" 推荐价值客户匹配表(RFM_RTL_RECOM_MATCH)删除成功= %s",
                                 corp_name + " - " + self.busidate + "-" + str(ret))
                PRE_SQL = "INSERT INTO RFM_RTL_RECOM_MATCH(BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, PROV_CODE," \
                          " PROV_NAME, CITY_CODE,CITY_NAME,ALL_RTL_NUM,ZY_RTL_NUM,HXH_RTL_NUM,RECOM_RTL_NUM," + \
                          " MATCH_RTL_NUM,MATCH_RATIO,HXH_RTL_INNER_NUM,MATCH_RTL_INNER_NUM,MATCH_INNER_RATIO," + \
                          " HXH_RTL_OUTER_NUM, MATCH_RTL_OUTER_NUM, MATCH_OUTER_RATIO, TIMES_TYPE) VALUES "
                ret = 0
                for row in zh_df.index:
                    prov_code = zh_df.loc[row,'PROV_CODE']
                    pro_name = zh_df.loc[row,'PROV_NAME']
                    city_code = zh_df.loc[row,'CITY_CODE']
                    city_name = zh_df.loc[row,'CITY_NAME']
                    all_rtl_num = str(zh_df.loc[row,'ALL_RTL_NUM'])
                    zy_rtl_num = str(zh_df.loc[row,'ZY_RTL_NUM'])
                    hxh_rtl_inner_num = str(zh_df.loc[row,'HXH_RTL_INNER_NUM'])
                    hxh_rtl_outer_num = str(zh_df.loc[row,'HXH_RTL_OUTER_NUM'])
                    hxh_rtl_num = str(zh_df.loc[row,'HXH_RTL_NUM'])
                    recom_rtl_num = str(zh_df.loc[row,'RECOM_RTL_NUM'])
                    match_rtl_inner_num = str(zh_df.loc[row,'MATCH_RTL_INNER_NUM'])
                    match_rtl_outer_num = str(zh_df.loc[row,'MATCH_RTL_OUTER_NUM'])
                    match_rtl_num = str(zh_df.loc[row,'MATCH_RTL_NUM'])
                    match_ratio = '0.0'
                    if zh_df.loc[row,'HXH_RTL_NUM'] != 0.0 :
                        match_ratio = str(zh_df.loc[row,'MATCH_RTL_NUM']/zh_df.loc[row,'HXH_RTL_NUM'])
                    pass
                    match_inner_ratio = '0.0'
                    if zh_df.loc[row,'HXH_RTL_INNER_NUM'] != 0.0 :
                        match_inner_ratio = str(zh_df.loc[row,'MATCH_RTL_INNER_NUM']/zh_df.loc[row,'HXH_RTL_INNER_NUM'])
                    pass
                    match_outer_ratio = '0.0'
                    if zh_df.loc[row,'HXH_RTL_OUTER_NUM'] != 0.0 :
                        match_outer_ratio = str(zh_df.loc[row,'MATCH_RTL_OUTER_NUM']/zh_df.loc[row,'HXH_RTL_OUTER_NUM'])
                    pass
                    i_sql = i_sql + " ('" + self.busidate + "', '" + corp_code + "', '" + corp_name + "', '" + \
                            group_code + "', '" + prov_code + "', '" + pro_name + "', '" + \
                            city_code + "', '" + city_name + "', " + all_rtl_num + "," + zy_rtl_num + "," + \
                            hxh_rtl_num + "," + recom_rtl_num + "," + match_rtl_num + "," + match_ratio  + "," + \
                            hxh_rtl_inner_num + "," + match_rtl_inner_num + "," + match_inner_ratio + ","  + \
                            hxh_rtl_outer_num + "," + match_rtl_outer_num + "," + match_outer_ratio + ",'" +\
                            str(ratio) + "'),"
                pass
                if (i_sql != ""):
                    i_sql = i_sql.rstrip(',')
                    i_sql = PRE_SQL + i_sql
                    ret = db.insert(i_sql)
                    i_sql = ""
                pass
                self.logger.info(" 推荐价值客户匹配表(分地市)成功记录数=%s",str(ret))
            pass
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",i_sql)
        finally:
            db.close()
        pass
    pass
    ##########################################
    # 各中烟拜访人匹配绩效表(分地市)
    ##########################################
    def doVistorRtlMatch(self, corp_code, corp_name, group_code, busi_date):

        db = MysqlDB(logger=self.logger)
        i_sql = ""
        try:
            db.connect()
            # 1.获取数据
            # 1.1 核心户
            s_sql = "SELECT PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME, VISIT_CODE, VISIT_NAME," + \
                    " VISIT_TYPE, R_KHTYBM,Z_RANK,MATCH_TYPE " + \
                    " FROM RFM_VALUE_RTL_RECOM " + \
                    " WHERE BUSI_DATE='" + busi_date + "' and CORP_CODE='" + corp_code + "' and " + \
                    " GROUP_CODE='" + group_code + "' " + \
                    " order by PROV_CODE, CITY_CODE,Z_RANK "
            rec2 = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            all_recom_df = pd.DataFrame(list(rec2), columns=['PROV_CODE', 'PROV_NAME','CITY_CODE', 'CITY_NAME','VISIT_CODE',
                                                       'VISIT_NAME', 'VISIT_TYPE','R_KHTYBM','Z_RANK','MATCH_TYPE'])
            all_recom_df.fillna(0.0, inplace=True)
            self.logger.info(" all_recom_df=%s",all_recom_df.columns)
            self.logger.info(" all_recom_df=%s",len(all_recom_df))

            # 1.2 拜访人的拜访户
            s_sql = "SELECT PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME, VISIT_CODE, VISIT_NAME," +\
                    " VISIT_TYPE, count(R_KHTYBM) VISIT_NUM " + \
                    " FROM RFM_CORP_RTL_VALID " + \
                    " WHERE BUSI_DATE='" + self.busidate + "' and CORP_CODE='" + corp_code + "' and " + \
                    " GROUP_CODE='" + group_code + "' " + \
                    " group by  PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME, VISIT_CODE, VISIT_NAME,VISIT_TYPE" +\
                    " order by CITY_CODE, VISIT_CODE "
            rec2 = db.select(s_sql)
            # self.logger.info(" SQL=:\n%s",s_sql)
            visit_df = pd.DataFrame(list(rec2), columns=['PROV_CODE', 'PROV_NAME','CITY_CODE', 'CITY_NAME','VISIT_CODE',
                                                         'VISIT_NAME', 'VISIT_TYPE', 'VISIT_NUM'])
            visit_df.fillna(0.0, inplace=True)

            # 1.3 计算实际拜访的总户数
            s_sql = "SELECT CITY_CODE, count(distinct R_KHTYBM) BF_NUM " + \
                    " FROM RFM_CORP_RTL_VALID " + \
                    " WHERE BUSI_DATE='" + self.busidate + "' and CORP_CODE='" + corp_code + "' and " + \
                    " GROUP_CODE='" + group_code + "' " + \
                    " group by  CITY_CODE "
            rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            bfzs_df = pd.DataFrame(list(rec), columns=['CITY_CODE','BF_NUM'])
            bfzs_df.fillna(0.0, inplace=True)
            self.logger.info(" bfzs_df=%s",bfzs_df.columns)
            self.logger.info(" bfzs_df=%s",len(bfzs_df))

            PRE_SQL = "INSERT INTO RFM_VISITOR_MATCH_CITY(BUSI_DATE, CORP_CODE, CORP_NAME, GROUP_CODE, PROV_CODE," \
                      " PROV_NAME, CITY_CODE,CITY_NAME,VISIT_CODE,VISIT_NAME,VISIT_TYPE,VISIT_RTL_NUM," + \
                      " RECOM_RTL_NUM,MATCH_RTL_NUM, MATCH_RATIO,TIMES_TYPE) VALUES "
            cnt = 0
            # 按不同比例加工处理
            for ratio in self.RATIOS:
                # 2.0 插入前先删除推荐价值客户匹配表(分地市)
                d_sql = "delete from RFM_VISITOR_MATCH_CITY where BUSI_DATE = '" + self.busidate + "' " + \
                        " and CORP_CODE = '" + corp_code + "' and GROUP_CODE = '" + group_code + "' and " + \
                        " TIMES_TYPE = '" + str(ratio) + "' "
                ret = db.delete(d_sql)
                self.logger.info(" 拜访人匹配绩效表(分地市)(RFM_VISITOR_MATCH_CITY)删除成功= %s",
                                 corp_name + " - " + self.busidate + "-" + str(ret))
                for row in bfzs_df.index:
                    num = bfzs_df.loc[row,'BF_NUM']
                    city_code = bfzs_df.loc[row,'CITY_CODE']
                    std_num = int(num * ratio) + 1
                    tmp_df = all_recom_df[all_recom_df['CITY_CODE'] == city_code][0:std_num]
                    tmp_df = tmp_df[tmp_df['MATCH_TYPE'] == '1']
                    tmp_df = tmp_df[['VISIT_CODE', 'R_KHTYBM']]

                    recom_group_df=tmp_df.groupby(tmp_df["VISIT_CODE"]).agg("count")
                    new_recom_df = pd.DataFrame(columns=['VISIT_CODE', 'MATCH_RTL_NUM'])
                    count = 0
                    for row in recom_group_df.index:
                        new_recom_df.loc[count]= [row,recom_group_df.loc[row,'R_KHTYBM']]
                        count += 1
                    pass
                    # 1.5 组合数据
                    zh_df = pd.merge(visit_df[visit_df['CITY_CODE']==city_code],
                                     new_recom_df, how='left', on=['VISIT_CODE'])

                    zh_df.fillna(0.0, inplace=True)
                    # self.logger.info(" zh_df=%s",zh_df.columns)
                    # self.logger.info(" zh_df=%s",len(zh_df))

                    ret = 0
                    for row in zh_df.index:
                        prov_code = zh_df.loc[row,'PROV_CODE']
                        pro_name = zh_df.loc[row,'PROV_NAME']
                        city_code = zh_df.loc[row,'CITY_CODE']
                        city_name = zh_df.loc[row,'CITY_NAME']
                        visit_code = zh_df.loc[row,'VISIT_CODE']
                        visit_name = zh_df.loc[row,'VISIT_NAME']
                        visit_type = zh_df.loc[row,'VISIT_TYPE']
                        visit_num = str(zh_df.loc[row,'VISIT_NUM'])
                        match_num = str(zh_df.loc[row,'MATCH_RTL_NUM'])
                        recom_num = str(std_num)
                        match_ratio = '0.0'
                        if zh_df.loc[row,'VISIT_NUM'] != 0.0 :
                            match_ratio = str(zh_df.loc[row,'MATCH_RTL_NUM']/zh_df.loc[row,'VISIT_NUM'])
                        pass
                        i_sql = i_sql + " ('" + self.busidate + "', '" + corp_code + "', '" + corp_name + "', '" + \
                                group_code + "', '" + prov_code + "', '" + pro_name + "', '" + \
                                city_code + "', '" + city_name + "', '" + visit_code + "','" + visit_name + "','" + \
                                visit_type + "'," + visit_num + "," + recom_num + "," + match_num  + "," + \
                                match_ratio +  ",'" + str(ratio) + "'),"
                        cnt = cnt + 1
                        if cnt%2000 == 0:
                            i_sql = i_sql.rstrip(',')
                            i_sql = PRE_SQL + i_sql
                            # self.logger.info(" i_sql = :%s\n",i_sql)
                            ret = db.insert(i_sql)
                            i_sql = ""
                        pass
                    pass
                pass
                if (i_sql != ""):
                    i_sql = i_sql.rstrip(',')
                    i_sql = PRE_SQL + i_sql
                    ret = db.insert(i_sql)
                    i_sql = ""
                pass
                self.logger.info(" 拜访人匹配绩效表(分地市)(RFM_VISITOR_MATCH_CITY)成功记录数=%s",str(cnt))
            pass
        except Exception as ex:
            self.logger.error(" 数据库错误:\n%s",str(ex))
            self.logger.error(" sql = :\n%s",i_sql)
        finally:
            db.close()
        pass
    pass
pass
if __name__ == "__main__":
    # # data = ['201910','201909','201908','201907','201906','201905','201904','201903','201902','201901']
    # data = ['201901','201902','201903','201904','201905','201906','201907','201908','201909','201910']
    # data = ['201910','201909','201908',]
    # for row in data:
    #     rfm = RFMRtl()
    #     rfm.main(row)
    # pass
    rfm = RFMRtlKhHBZY()
    rfm.doProcess()
pass
