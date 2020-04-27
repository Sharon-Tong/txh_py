#  -*- coding: utf-8 -*-
# Project: BrandAnalyze
# Author:  yyg
# Created on 2019-08-10
#########################################################
import numpy as np
from com.ctitc.bigdata.db.mysqldb import MysqlDB
from com.ctitc.bigdata.common.log.mylog import MyLog
import pandas as pd
import numpy.linalg as nlg
import math
from numpy import eye, asarray, dot, sum, diag
from numpy.linalg import svd
from factor_analyzer import FactorAnalyzer
import datetime
from dateutil.relativedelta import relativedelta
import statsmodels.formula.api as smf

from sklearn.linear_model import LinearRegression
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor
from patsy.highlevel import dmatrices
from scipy.optimize import minimize
from com.ctitc.bigdata.db.hivedb import HiveDB

from com.ctitc.bigdata.util.dateutil import DateUtil
#########################################################
# 市场健康状态评价分析及模拟仿真分析模型（RAS）父类
#########################################################
class RASBase():
    logger = None
    ##########################################
    # 初始化
    ##########################################
    def __init__(self, logkey="ras_revalue", action_type='1'):
        self.cur_rec = None
        self.X = None
        self.X_std = None
        self.X_row = 0
        self.X_col = 0
        self.logger = MyLog.getLogger(logkey, log_file='logger_ras.conf')
        self.action_type = action_type
    pass

    ##########################################
    # 从大数据平台获取地市级数据指标信息
    # datasource:0:mysql,1:hive
    ##########################################
    def getCityInfoFromHive(self, sql_condition, datasource='0'):
        # 1.0 获取数据源
        db = None
        if datasource == '1':
            db = HiveDB(logger=self.logger)
        else:
            db = MysqlDB(logger=self.logger)
        pass
        # 0.获取当前月份
        cur_ym = DateUtil.get_nowym()

        # 1.0 从大数据平台获取信息
        data = None
        rec = None
        try:
            db.connect()
            # 1.查询需要处理的规格
            # 卷烟动销率=本周期卷烟实际销量÷（期初库存+本期购进）
            s_sql = "select CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, BUSI_DATE,  " \
                    " FACTOR_XLZB, (1.0/FACTOR_SYCXB) DXL, FACTOR_SCFE,  FACTOR_CGL, FACTOR_PHL," \
                    " FACTOR_HJXL, FACTOR_LSHS " \
                    " from RAS_FT_SRC_CITY_MONTH " \
                    "  WHERE FACTOR_XL > 0.0 and BUSI_DATE < '" + cur_ym + "'" + \
                    " and  BAR_CODE in (" + sql_condition  + ")  order by  BAR_CODE, CITY_CODE, BUSI_DATE  "
            rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            data = pd.DataFrame(list(rec), columns=['CITY_CODE', 'CITY_NAME', 'BAR_CODE', 'BAR_NAME', 'BUSI_DATE',
                                                    'FACTOR_XLZB', 'DXL', 'FACTOR_SCFE','FACTOR_CGL', 'FACTOR_PHL',
                                                    'FACTOR_HJXL', 'FACTOR_LSHS'])
            # 去重,是不是与LEVEL_CODE相关?,2019-11-23
            # 屏蔽掉检查数据是不是有重复
            # data.drop_duplicates(subset=['CITY_CODE', 'BAR_CODE', 'BUSI_DATE'],inplace=True)
            data.fillna(0.0, inplace=True)
        except Exception as ex:
            self.logger.error(" 大数据平台数据库错误:\n%s",str(ex))
        finally:
            db.close()
        pass
        return data
    pass
    ##########################################
    # 从大数据平台获取地市级数据指标信息
    # datasource:0:mysql,1:hive
    ##########################################
    def getCityInfoFromHive_AUTO(self, group_code, sql_condition, datasource='0'):
        reverse_ft = self.getReverseFeatures(group_code)
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
                if second_code in reverse_ft:
                    sql_ft = "1.0/" + second_code
                else:
                    sql_ft = second_code
                pass
            else:
                if second_code in reverse_ft:
                    sql_ft = sql_ft +  ", 1.0/"  + second_code
                else:
                    sql_ft = sql_ft +  ", "  + second_code
                pass
            pass
            pd_cols = pd_cols + ", '" + second_code + "'"
        pass
        # 1.0 获取数据源
        db = None
        if datasource == '1':
            db = HiveDB(logger=self.logger)
        else:
            db = MysqlDB(logger=self.logger)
        pass
        # 0.获取当前月份
        cur_ym = DateUtil.get_nowym()

        # 1.0 从大数据平台获取信息
        data = None
        rec = None
        try:
            db.connect()
            # 1.查询需要处理的规格
            # 卷烟动销率=本周期卷烟实际销量÷（期初库存+本期购进）
            s_sql = "select CITY_CODE, CITY_NAME, BAR_CODE, BAR_NAME, BUSI_DATE,  " + sql_ft + \
                    " from RAS_FT_SRC_CITY_MONTH " \
                    "  WHERE FACTOR_XL > 0.0 and BUSI_DATE < '" + cur_ym + "'" + \
                    " and BAR_CODE in (" + sql_condition  + ")  order by  BAR_CODE, CITY_CODE, BUSI_DATE  "
            rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            cols = "['CITY_CODE', 'CITY_NAME', 'BAR_CODE', 'BAR_NAME', 'BUSI_DATE'" + pd_cols + "]"
            data = pd.DataFrame(list(rec), columns=eval(cols))
            data.fillna(0.0, inplace=True)
        except Exception as ex:
            self.logger.error(" 大数据平台数据库错误:\n%s",str(ex))
        finally:
            db.close()
        pass
        return rec, data
    pass

    ##########################################
    # 从大数据平台获取地市级数据指标信息
    # datasource:0:mysql,1:hive
    ##########################################
    def getProvInfoFromHive(self, sql_condition, datasource='0'):
        # 1.0 获取数据源
        db = None
        if datasource == '1':
            db = HiveDB(logger=self.logger)
        else:
            db = MysqlDB(logger=self.logger)
        pass

        # 0.获取当前月份
        cur_ym = DateUtil.get_nowym()

        # 1.0 从大数据平台获取信息
        data = None
        rec = None
        try:
            db.connect()
            # 1.查询需要处理的规格
            # 卷烟动销率=本周期卷烟实际销量÷（期初库存+本期购进）
            # s_sql = "select PROV_CODE, PROV_NAME, BAR_CODE, BAR_NAME, BUSI_DATE,  " \
            #         " FACTOR_XLZB, (FACTOR_XL/FACTOR_KCL) DXL, FACTOR_SCFE,  FACTOR_CGL, FACTOR_PHL," \
            #         " FACTOR_HJXLB, FACTOR_LSHSB " \
            #         " from RAS_FT_SRC_PROV_MONTH " \
            #         "  WHERE FACTOR_XL > 0.0 and " + sql_condition  + " order by  BAR_CODE, PROV_CODE, BUSI_DATE  "
            s_sql = "select PROV_CODE, PROV_NAME, BAR_CODE, BAR_NAME, BUSI_DATE,  " \
                    " FACTOR_XLZB, (FACTOR_XL/FACTOR_KCL) DXL, FACTOR_SCFE,  FACTOR_PHL," \
                    " FACTOR_HJXLB, FACTOR_LSHSB " \
                    " from RAS_FT_SRC_PROV_MONTH " \
                    "  WHERE FACTOR_XL > 0.0 and BUSI_DATE < '" + cur_ym + "'" + \
                    " and " + sql_condition  + " order by  BAR_CODE, PROV_CODE, BUSI_DATE  "
            rec = db.select(s_sql)
            self.logger.info(" SQL=:\n%s",s_sql)
            data = pd.DataFrame(list(rec), columns=['PROV_CODE', 'PROV_NAME', 'BAR_CODE', 'BAR_NAME', 'BUSI_DATE',
                                                    'FACTOR_XLZB', 'DXL', 'FACTOR_SCFE','FACTOR_PHL',
                                                    'FACTOR_HJXLB', 'FACTOR_LSHSB'])
            data.fillna(0.0, inplace=True)
        except Exception as ex:
            self.logger.error(" 大数据平台数据库错误:\n%s",str(ex))
        finally:
            db.close()
        pass
        return rec, data
    pass

    #########################################
    # 定义方差最大旋转函数
    ##########################################
    def varimax(self, Phi, gamma = 1.0, q =20, tol = 1e-6):
        p,k = Phi.shape # 给出矩阵Phi的总行数，总列数
        R = eye(k) # 给定一个k*k的单位矩阵
        d = 0
        for i in range(q):
            d_old = d
            Lambda = dot(Phi, R) # 矩阵乘法
            # 奇异值分解svd
            u,s,vh = svd(dot(Phi.T,asarray(Lambda)**3 - (gamma/p) * dot(Lambda, diag(diag(dot(Lambda.T,Lambda))))))
            R = dot(u,vh) # 构造正交矩阵R
            d = sum(s) # 奇异值求和
            if d_old!=0 and d/d_old:
                return dot(Phi, R) # 返回旋转矩阵Phi*R
            pass
    pass
    #########################################
    # 因子分析
    # 返回PCA得分、因子得分
    ##########################################
    def getScore(self, X_std, C, ratio, grp_code, grp_name, tbl_name):

        # 1. 公共处理和因子分析假设条件检验(6个)
        # 1.0 KMO检验
        kmo = self.kmo(C)
        self.logger.info("KMO检验值:%s",kmo)

        # 1.1 计算特征值和特征向量
        eig_value, eig_vector = nlg.eig(C)
        eig_value = eig_value.real
        eig_vector = eig_vector.real
        # 利用变量名和特征值建立一个数据框
        eig = pd.DataFrame()
        # 列名
        eig['names'] = X_std.columns
        # 特征值
        eig['eig_value'] = eig_value
        max_factor_num = len(eig_value)
        self.logger.info("特征值:\n%s",eig_value)
        self.logger.info("特征向量:\n%s",eig_vector)
        self.logger.info("最大特征数:\n%s",max_factor_num)
        eig.sort_values('eig_value', ascending=False, inplace=True)
        self.logger.info("排序后的特征值:\n%s", eig)

        # 1.2 求因子模型的因子载荷阵，寻找公共因子个数k
        com_factor_num = 0
        for k in range(1,max_factor_num):
            if eig['eig_value'][:k].sum()/eig['eig_value'].sum() >= ratio:
                com_factor_num = k
                break
            pass
        pass
        # 1.2.1 排序后的特征值以及特征向量
        # 对特征值从小到大排序
        eigValIndice = np.argsort(eig_value)
        self.logger.info("对特征值从小到大排序:\n%s", eigValIndice)
        # 最大的n个特征值的下标
        n_eigValIndice = eigValIndice[-1:-(com_factor_num + 1):-1]
        self.logger.info("最大的n个特征值的下标:\n%s", n_eigValIndice)
        # 最大的n个特征值
        n_eigValue = eig_value[n_eigValIndice]
        self.logger.info("最大的n个特征值:\n%s", n_eigValue)
        # 最大的n个特征值对应的特征向量
        n_eigVect = eig_vector[:,n_eigValIndice]
        self.logger.info("最大的n个特征值对应的特征向量:\n%s", n_eigVect)


        # 1.3 计算各主成分方差贡献
        contribute = []
        self.logger.info("公共因子个数:%s", com_factor_num)
        for i in range(com_factor_num):
            # contribute.append(eig_value[i] / np.sum(eig_value))
            contribute.append(eig['eig_value'][i] / eig['eig_value'].sum())
        pass
        self.logger.info("各主成分方差贡献:\n%s", contribute)

        # 1.4 因子载荷矩阵，只是因子
        # 存在特征根为负的情况
        A  = np.zeros((max_factor_num, com_factor_num))
        for i in range(com_factor_num):
            if n_eigValue[i] >= 0:
                A[:,i] = math.sqrt(n_eigValue[i])*n_eigVect[:,i]
            pass
            # A[:,i] = math.sqrt(eig_value[i])*eig_vector[:,i]
        pass
        a = pd.DataFrame(A)
        # a = ""
        self.logger.info("因子载荷矩阵(未旋转):\n%s", a)
        # print("\n因子载荷矩阵(未旋转):\n", a)

        # 2. 计算因子综合得分,(加载因子分析模块)
        # 2.0 公共因子个数为com_factor_num, 正交旋转Varimax法
        # method : {'minres', 'ml', 'principal'}, optional, The fitting method to use, either MINRES or
        # Maximum Likelihood. Defaults to 'minres'.
        fa = FactorAnalyzer(n_factors=com_factor_num, method='minres', rotation='varimax')
        fa.fit(X_std)

        # 2.0 特征值; 特征向量
        eig_original,eig_common  = fa.get_eigenvalues()
        self.logger.info("初始特征值:\n%s", eig_original)
        self.logger.info("共通因子特征值:\n%s", eig_common)
        # print(" 初始特征值:\n", eig_original)
        # print(" 共通因子特征值:\n", eig_common)

        # 2.1 公因子方差:变量共同度量表, 反映提取的公因子对原始变量解释力的强度,0.9以上强
        h = fa.get_communalities()
        self.logger.info("公因子方差:\n%s", h)
        # print("公因子方差:\n", h)

        # 2.2 成分矩阵(旋转后)
        #         Factor1   Factor2
        # XL      0.987511 -0.005429
        # XLZB    0.987511 -0.005429
        # SCFE    0.697608  0.039525
        # XLZJ    0.360671  0.932757
        # SCFEZJ -0.197689  0.654072
        loading = fa.loadings_
        self.logger.info("成分矩阵:\n%s", loading)
        # print("\n成分矩阵:\n", loading)

        # 2.3 解释的总方差(贡献率)
        #                   Factor1   Factor2
        # SS Loadings     2.606178  1.299467
        # Proportion Var  0.521236  0.259893
        # Cumulative Var  0.521236  0.781129
        var = fa.get_factor_variance()
        self.logger.info("解释的总方差（即贡献率）:\n%s", var)
        # print("\n解释的总方差（即贡献率）:\n", var)
        # 2.4 因子得分
        #     Factor1   Factor2
        # 0    5.319680 -2.966405
        # 1    1.113992  0.921729
        # 2    0.459738  0.155349
        # 3   -0.532002 -1.965364
        fa_score = fa.transform(X_std)
        # print(fa_score)
        # 2.5 综合得分
        # 将各因子乘上他们的贡献率除以总的贡献率,得到因子得分中间值
        fa_tmp_score = (fa_score*var[1])/var[-1][-1]
        # 将各因子得分中间值相加，得到综合得分
        fa_tmp_score = pd.DataFrame(fa_tmp_score)
        fa_t_score = fa_tmp_score.apply(lambda x: x.sum(), axis=1)
        fa_t_score = np.asarray(fa_t_score)
        # print("\n因子综合得分:\n", fa_t_score)

        # 3. 计算主成分综合得分
        # 采用主成分方法-- start
        # pca = PCA(n_components=com_factor_num)
        # pca.fit(X_std)
        # #report eigenvalue
        # eigenvalue = pca.explained_variance_
        # print("\n特征值:\n", eigenvalue)
        # # 每个主成分占方差比例
        # eigenvalue_var = pca.explained_variance_ratio_
        # print(eigenvalue_var)
        # contribute = eigenvalue_var
        # self.logger.info("每个主成分占方差比例:\n%s", contribute)
        # #report% cumulative variance
        # eigenvalue_var_ =np.cumsum(np.round(pca.explained_variance_ratio_, decimals=4)*100)
        # print(eigenvalue_var_)
        # #report eigenvector
        # eigenvector = pca.components_
        # self.logger.info("主成分特征向量:\n%s", eigenvector)
        # n_eigVect = np.asarray(n_eigVect)
        # print("\n主成分特征向量:\n", eigenvector)
        # self.logger.info("主成分特征向量:\n%s", n_eigVect)
        # 采用主成分方法-- end

        # 矩阵化处理
        X1 = np.mat(X_std)
        # 3.1 计算主成分得分
        # pca_score = (X1).dot(eig_vector[:,:com_factor_num])
        pca_score = (X1).dot(n_eigVect[:,:com_factor_num])
        # print(pca_score)
        # 3.2 计算综合得分
        contr_s = np.asarray(contribute)
        contribute = np.reshape(contr_s,(com_factor_num,1))
        pca_t_score = (pca_score).dot(contribute) / np.sum(contribute)
        pca_t_score = np.asarray(pca_t_score)
        # print("\n主成分综合得分:\n", pca_t_score)
        self.logger.info("主成分综合得分尺寸:\n%s", pca_t_score.shape)
        # print("\n主成分综合得分尺寸:\n", pca_t_score.shape)

        # 4. 结果保存到数据库中
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            # 1.1 插入前先删除成分载荷表
            d_load_sql = "delete from " + tbl_name + " where GROUP_CODE='" + grp_code + "'"
            db.delete(d_load_sql)
            # 1.2 插入成分载荷表
            i_load_sql = "INSERT INTO " + tbl_name + " (GROUP_CODE, GROUP_NAME, EIG_VALUE, EIG_VECTOR, COM_MATRIX, CONTRIBUTE, " \
                                                     " FACTOR_NUM, COMMON_FACTOR_NUM, KMO,LOADING,COMMUNALITIES,FACTOR_VARIANCE) " \
                                                     " VALUES ('" + grp_code + "', '"  + grp_name + "', " \
                                                                                                    " '" + str(eig_value) + "', '" + str(eig_vector) + "','"  + str(a) + "'," \
                                                                                                                                                                         " '" + str(contr_s) + "', " + str(max_factor_num) + "," + str(com_factor_num) + "," + str(kmo) + \
                         ", '" + str(loading) + "','" + str(h) + "','" + str(var) + "') "
            results = db.insert(i_load_sql)
        finally:
            db.close()
        pass
        return pca_t_score, fa_t_score
    pass

    #########################################
    # 根据任务号,获取一/二级指标
    ##########################################
    def getFeatures(self, group_code):
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            # 2.查询需要处理的任务的一/二级指标
            s_ft = "select GROUP_CODE, FIRST_FT_CODE, FIRST_FT_NAME, SECOND_FT_CODE, SECOND_FT_NAME " \
                   + " from RAS_FEATURE_CONFIG  where IS_USE = '1' and GROUP_CODE = '" + group_code + "' " \
                   + " order by  GROUP_CODE,  FIRST_FT_CODE, SECOND_FT_CODE "
            fts = db.select(s_ft)
            return fts
        finally:
            db.close()
        pass
    pass
    #########################################
    # 根据任务号,获取反向指标
    ##########################################
    def getReverseFeatures(self, group_code):
        db = MysqlDB(logger=self.logger)
        rec = None
        try:
            db.connect()
            # 2.查询需要处理的任务的一/二级指标
            s_ft = "select GROUP_CODE, SECOND_FT_CODE, SECOND_FT_NAME " \
                   + " from RAS_FEATURE_CONFIG  where IS_USE = '1' and GROUP_CODE = '" + group_code + "' " \
                   + " and IS_REVERSE='1' order by  GROUP_CODE,  FIRST_FT_CODE "
            rec = db.select(s_ft)
        finally:
            db.close()
        pass
        reverse_fts = []
        for row in rec:
            second_code = row[1]
            reverse_fts.append(second_code)
        pass
        return reverse_fts
    pass
    #########################################
    # 根据任务号,获取一级指标
    ##########################################
    def getFirstFeatures(self, group_code):
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            # 2.查询需要处理的任务的一/二级指标
            s_ft = "select distinct GROUP_CODE, FIRST_FT_CODE, FIRST_FT_NAME " \
                   + " from RAS_FEATURE_CONFIG  where IS_USE = '1' and GROUP_CODE = '" + group_code + "' " \
                   + " order by  GROUP_CODE,  FIRST_FT_CODE "
            fts = db.select(s_ft)
            return fts
        finally:
            db.close()
        pass
    pass
    #########################################
    # 根据任务号,获取城市代码列表
    ##########################################
    def getCityCodes(self, group_code, pro_code='0', table_name=''):
        db = MysqlDB(logger=self.logger)
        try:
            db.connect()
            # 2.查询需要处理的任务的城市代码列表
            s_code = "select distinct CITY_CODE, CITY_NAME from " + table_name + " " \
                                                                                 " where GROUP_CODE = '" + group_code + "' and PRO_CODE = '" + pro_code + "' " \
                                                                                                                                                          " order by  CITY_CODE "
            codes = db.select(s_code)
            return codes
        finally:
            db.close()
        pass
    pass
    #########################################
    # KMO（Kaiser-Meyer-Olkin)检验统计量是用于
    # 比较变量间简单相关系数和偏相关系数的指标
    # 常用的kmo度量标准:　0.9以上表示非常适合；0.8表示适合；
    # 0.7表示一般；0.6表示不太适合；0.5以下表示极不适合
    ##########################################
    def kmo(self, corr):
        try:
            corr_inv = np.linalg.inv(corr)
            nrow_inv_corr, ncol_inv_corr = corr.shape
            A = np.ones((nrow_inv_corr,ncol_inv_corr))
            for i in range(0,nrow_inv_corr,1):
                for j in range(i,ncol_inv_corr,1):
                    A[i,j] = -(corr_inv[i,j])/(math.sqrt(abs(corr_inv[i,i]*corr_inv[j,j])))
                    A[j,i] = A[i,j]
                pass
            pass
            dataset_corr = np.asarray(corr)
            kmo_num = np.sum(np.square(dataset_corr)) - np.sum(np.square(np.diagonal(A)))
            kmo_denom = kmo_num + np.sum(np.square(A)) - np.sum(np.square(np.diagonal(A)))
            kmo_value = kmo_num / kmo_denom
            return kmo_value
        except Exception as ex:
            self.logger.error(" kmo计算错误:\n%s",str(ex))
            return 0.0
            # print(str(ex))
        pass
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
    ###################################################################
    # 前向逐步回归算法
    # 向前法：向前法的思想是变量由少到多，每次增加一个，直至没有可引入的变量为止
    # 使用Adjusted R-squared来评判新加的参数是否提高回归中的统计显著性
    ####################################################################
    def forward_selected(self, data, response):
        """Linear model designed by forward selection.
        Parameters:
        -----------
        data : pandas DataFrame with all possible predictors and response
        response: string, name of response column in data
        Returns:
        --------
        model: an "optimal" fitted statsmodels linear model
               with an intercept
               selected by forward selection
               evaluated by adjusted R-squared
        """
        remaining = set(data.columns)
        remaining.remove(response)
        # self.logger.info(" 选择前的变量=%s\n",remaining)

        # 选择后的变量
        selected = []
        current_score, best_new_score = float('inf'), float('inf')
        # while remaining and current_score == best_new_score:
        # while remaining and current_score == best_new_score and count > 0:
        while remaining:
            scores_with_candidates = []
            for candidate in remaining:
                formula = "{} ~ {} + 1".format(response, ' + '.join(selected + [candidate]))
                score = smf.ols(formula, data).fit().aic

                # score = smf.ols(formula, data).fit().rsquared_adj
                scores_with_candidates.append((score, candidate))
            pass
            scores_with_candidates.sort(reverse=True)
            best_new_score, best_candidate = scores_with_candidates.pop()
            if current_score > best_new_score:
                remaining.remove(best_candidate)
                selected.append(best_candidate)
                current_score = best_new_score
                # self.logger.info(" aic is=%s",str(current_score))
                # self.logger.info(" best_candidate=%s",best_candidate)
                # print("aic is {},continuing!".format(current_score))  #输出最小的aic值
            else:
                # print("for selection over!")
                break
            pass
        pass
        formula = "{} ~ {} + 1".format(response, ' + '.join(selected))
        model = smf.ols(formula, data).fit()
        return model, selected
        # return formula, selected
    pass
    ###################################################################
    # V.I.F. = 1 / (1 - R^2)
    # The Variance Inflation Factor (VIF) is a measure of colinearity
    # among predictor variables within a multiple regression.
    # It is calculated by taking the the ratio of the variance of all
    # a given model's betas divide by the variane of a single beta
    # if it were fit alone.
    # 方差膨胀系数(variance inflation factor，VIF)
    # 是衡量多元线性回归模型中复 (多重)共线性严重程度的一种度量。
    # 它表示回归系数估计量的方差与假设自变量间不线性相关时方差相比的比值。
    # 通常以10作为判断边界。当VIF<10,不存在多重共线性；当10<=VIF<100,
    # 存在较强的多重共线性；当VIF>=100, 存在严重多重共线性。
    ####################################################################
    def countVIF(self, data, response):
        #gather features
        cols = list(data.columns)
        cols.remove(response)
        feature = " + ".join(cols)
        # features  = filter(lambda x: x != response, cols)
        # get y and X dataframes based on this regression:
        y, X = dmatrices(response + ' ~ ' + feature, data, return_type='dataframe')
        # For each X, calculate VIF and save in dataframe
        vif = pd.DataFrame()
        vif["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
        vif["features"] = X.columns
        vif.fillna('null', inplace=True)
        # OLS模型训练
        # mod=sm.OLS(y,X)
        # res=mod.fit()
        # result = res.summary()
        # model = {
        #     'n': int(res.nobs),
        #     'df': res.df_model,
        #     'r': math.sqrt(res.rsquared),
        #     'r_squared':res.rsquared,
        #     'r_squared_adj': res.rsquared_adj,
        #     'f_statistic': res.fvalue, # F检验
        #     'prob_f_statistic': res.f_pvalue
        # }
        # coefficient = {
        #     'coefficient':list(res.params),
        #     # 'std': list(np.diag(np.sqrt(res.cov_params()))),
        #     't': list(res.tvalues),
        #     'sig': [i for i in map(lambda x:float(x),("".join("{:.4f},"*len(res.pvalues)).format(*list(res.pvalues))).rstrip(",").split(","))]
        # }
        # param = {'model': model, 'coefficient': coefficient}
        # return vif, param
        return vif
    pass
    ###################################################################
    # 非线性规划求解
    # param_df:max:bounds上限,min:bounds下限,arg:系数,b0:初始值
    # 如:bounds=((None,0),(0,None),(None,None))
    # interc:常数项,截距
    # score:目标值
    # res.x:最优解,x = [ 2.59153833  2.40568468  0.38390532  0.98 ]
    ####################################################################
    def getNLP(self, param_df, score, interc):
        bnds = []
        formula = ""
        count = 0
        x0 = []
        self.logger.info(" param_df = \n%s",param_df)
        for row in param_df.index:
            max = param_df.loc[row]['max']
            min = param_df.loc[row]['min']
            arg = param_df.loc[row]['arg']
            b0 = param_df.loc[row]['b0']
            bnds.append((min,max))
            x0.append(b0)
            formula = formula + str(arg) + " * x[" + str(count) + "] + "
            count += 1
        pass
        formula = formula + str(interc)
        cons_formula = formula + " - " + str(score)
        self.logger.info(" cons_formula = %s",cons_formula)
        fun = lambda x : (eval(formula))
        cons_fun = lambda x : (eval(cons_formula))
        cons = ({'type': 'eq', 'fun': cons_fun})
        res = minimize(fun, x0, method='SLSQP', constraints=cons, bounds=bnds)
        return res.x
    pass

    ###################################################################
    # 月份计算,增加一个月
    ####################################################################
    def getAddMonth(self, smonth):
        date = datetime.datetime.strptime(smonth,'%Y%m')
        delta = date + relativedelta(months=1)
        rtn = delta.strftime('%Y%m')
        return rtn
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
    # def getAddMonthByStep(smonth, step):
    #     date = datetime.datetime.strptime(smonth,'%Y%m')
    #     delta = date + relativedelta(months=step)
    #     rtn = delta.strftime('%Y%m')
    #     return rtn
    # pass
    # tmp = getAddMonthByStep('201901',-2)
    # print(tmp)
    # def getNLP(param_df, score, b0):
    #     bnds = []
    #     formula = ""
    #     count = 0
    #     x0 = []
    #     for row in param_df.index:
    #         max = param_df.loc[row]['max']
    #         min = param_df.loc[row]['min']
    #         arg = param_df.loc[row]['arg']
    #         bnds.append((min,max))
    #         x0.append(min)
    #         formula = formula + str(arg) + " * x[" + str(count) + "] + "
    #         count += 1
    #     pass
    #     formula = formula + " + " + str(b0)
    #     cons_formula = formula + " - " + str(score)
    #     fun = lambda x : (eval(formula))
    #     cons_fun = lambda x : (eval(cons_formula))
    #     cons = ({'type': 'eq', 'fun': cons_fun})
    #     res = minimize(fun, x0, method='SLSQP', constraints=cons, bounds=bnds)
    #     return res.x
    # pass

    # df=pd.DataFrame()
    # df["min"]=[2.23,1.32,0.37,0.98,0.05,0.89,-0.49,0.76,-1.42,-0.02,-0.21]
    # df["max"]=[20.55,4.83,16.79,0.98,10.52,1.79,12.72,7.86,1.14,8.47,10.61]
    # df["arg"]=[0.1352,0.406,0.0052,-0.0005,0.0005,0.0676,-0.0014,0.5871,-0.0006,-0.0034,0.0026]
    # x = getNLP(df, 2.77, 1.0)
    # print(x)
    # x = np.array([[100,4,9.3],[50,3,4.8],[100,4,8.9],
    #               [100,2,6.5],[50,2,4.2],[80,2,6.2],
    #               [75,3,7.4],[65,4,6],[90,3,7.6],[90,2,6.1]])
    # X = x[:,:-1]
    # Y = x[:,-1]
    # print(X,Y)
    # regr = LinearRegression()
    # regr.fit(X,Y)
    # print('coefficients(b1,b2...):',regr.coef_)
    # print('intercept(b0):',regr.intercept_)
    # print('intercept(b0):',regr.score(X,Y))
    # print(regr.coef_[0])
    # param = {}
    # param['name'] = '123'
    # param['sez'] = '234'
    # cols = param.keys()
    # print("','".join(cols))
    # 新建一个空的DataFrame
    df1=pd.DataFrame()
    df=pd.DataFrame()
    #添加成绩单，最后显示成绩单表格
    # arr = ['16','90','97']
    # print("','".join(arr))
    # df["英语"]=[16,90,97,71,70,93,86,83,78,85,182]
    # df["经济数学"]=[65,95,51,74,78,63,91,82,75,71,55]
    # df["西方经济学"]=[93,81,76,88,66,79,83,92,78,86,78]
    # df["计算机应用基础"]=[85,78,81,95,70,67,82,72,80,81,77]
    df["c1"]=[16,90,97,71,70,93,86,83,78,85,182]
    df["c2"]=[65,95,51,74,78,None,91.5,82.00,75,71,55]
    df["c3"]=[93,81,76,88,66,79,83,92,78,86,78]
    df["c4"]=[85,78,81,95,70,67,82,72,80,81,None]
    df["c5"]=[1,1,1,2,2,2,3,3,3,3,3]
    df["c6"]=[1,1,1,2,2,2,2,2,1,1,2]

    print(df.index.values)
    df_1 = df[df['c6']==1]
    print(df_1.index.values)
    df_1 = df_1.reset_index(drop = True)
    print(df_1.index.values)
    # df_new = df[df['c5'] == 1]
    # print(df1.append(df))
    # tmp= df.groupby('c5')[['c5','c4','c3']].max()
    # tmp1 = pd.DataFrame(tmp)
    # for row in tmp1.index:
    #     print(tmp1.loc[row,'c5'])
    #     print(tmp1.loc[row,'c4'])
    # print(tmp1)
    # print(tmp1.columns)
    # test = df[['c2','c3']]
    # print(test)
    # all_cols = list(df.columns)
    # print(all_cols)
    # all_cols.remove('c4')
    # print(" + ".join(all_cols))
    # X_ALL = df.loc[:,all_cols]
    # print(X_ALL)
    #
    # all_cols = "('c1', 'c2')"
    # vif  = df.loc[:,eval(all_cols)]
    # print(vif)
    #
    #
    #
    # sub = df[df['c1'] == 90]['c3'].values[0]
    # print(sub)
    #
    # df1=pd.DataFrame()
    # df1["c1"]=[16,90,97,71,70,93,86,83,78,85,182]
    # df1["c2"]=[65,95,51,74,78,63,91,83,78,71,55]
    # df1["c3"]=[93,81,76,88,66,79,83,92,78,86,78]
    # df1["c4"]=[85,78,81,95,70,67,82,72,80,81,77]
    # tmp = df1['c4'].values
    # tmp = "('" + "','".join(tmp) + "')"
    # print(tmp)
    # d = df1[(df1['c3'] <= 88) & (df1['c2'] <= 70.75)]
    # print(d)
    # ratio = {'r_ratio': 0.1, 'f_ratio': 0.1, 'm_ratio': 0.1, 'x_ratio': 0.1, 'zje_ratio': 0.05, 'zdx_ratio': 0.05}
    # r_max = '29173'
    # print(r_max * 0.5)

    # print(str.find("1234")==2)
    # for indexs in df1.index:
    #     print(df1.loc[indexs]['c1'])
    # df2 = pd.merge(df, df1, on=['c1','c2'],how='inner')
    # # df2 = df1.join(df,on=['c1','c2','c3','c4'],how='inner')
    # print(df2)

    # # print('离散系数:',np.std(scores)/np.mean(scores))
    # param = df.describe()
    # print(param)
    # param.loc['cv'] = param.loc['std'] / param.loc['mean']
    # # 离散系数
    # param.loc['cv'] = param.loc['std'] / param.loc['mean']
    # # 中位线（IQR）：Q3-Q1上四分位数至下四分位数的距离
    # param.loc['IQR'] = param.loc['75%'] - param.loc['25%']
    # # 上限值：Q3+1.5×IQR
    # param.loc['uline'] = param.loc['75%'] + param.loc['IQR']*1.5
    # # 下限值：Q1-1.5×IQR
    # param.loc['dline'] = param.loc['25%'] - param.loc['IQR']*1.5
    #
    # # ret = plt.boxplot(x=df.values,labels=df.columns,whis=1.5)
    # print(param)
    # print(param.loc['dline','英语'])
    # cols = df.columns
    # print("+".join(cols))
    # a = filter(lambda x: x != '英语', cols)
    # print("+".join(a))
    # # df_new = pd.DataFrame()
    # # for col in cols:
    # #     up_value = param.loc['uline',col]
    # #     down_value = param.loc['dline',col]
    # #     print(col)
    # #     print(up_value)
    # #     print(down_value)
    # #     df[col] = df[col].apply(lambda x: up_value if x >= up_value else down_value if x <= down_value else x)
    # # pass
    # # print(df)

pass
