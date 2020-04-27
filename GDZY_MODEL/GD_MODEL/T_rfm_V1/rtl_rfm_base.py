# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/12 14:06 
'''

from GDZY_MODEL.GD_MODEL.T_rfm_V1.BASE.DataFuncForFrm import RfmDataFunct
from GDZY_MODEL.GD_MODEL.T_rfm_V1.rfm_config import RfmParam
from GDZY_MODEL.GD_MODEL.LOG.MyLog import logger
import pandas as pd
from GDZY_MODEL.GD_MODEL.T_rfm_V1.BASE.data_to_dd2 import data_to_db
from GDZY_MODEL.GD_MODEL.SQL_LINK.MySql_link import MysqlDB
from GDZY_MODEL.GD_MODEL.Sql_param.sqlconfig import sql_myself
import copy
class RtlRfmScore():

    def __init__(self):
        self.para  = RfmParam
        self.datafunc = RfmDataFunct()
        self.con = MysqlDB(sql_myself)
        self.con.connect()
        self.logger=logger.get_logger(self.para.logger_path)


    #####################################################################
    ##计算品牌下每个零售户的得分情况
    #####################################################################

    def get_rtl_brand_score(self,allorderdata,current_week,w_customize=[1/3,1/3,1/3]):
        # print('计算品牌。。。。。。')
        # print(allorderdata)

        bar_score_cv, bar_score_avg,C,D = self.get_rtl_bar_score(allorderdata,current_week,w_customize)

        # 获取R、F、M
        allorderdata = self.datafunc.get_rtl_r_f_m(allorderdata, current_week)

        # 获取变异系数权重
        allorderdata_avg_new = copy.copy(allorderdata)
        allorderdata_cv_new = self.datafunc.cv_weight(allorderdata)


        # 数据标准化
        allorderdata_cv_new = self.datafunc.max_min_Standar(allorderdata_cv_new)
        allorderdata_avg_new = self.datafunc.max_min_Standar(allorderdata_avg_new)

        #自定义权重数据
        #allorderdata_avg = allorderdata
        allorderdata_avg_new['R_W'] = w_customize[0]
        allorderdata_avg_new['F_W'] = w_customize[1]
        allorderdata_avg_new['M_W'] = w_customize[1]

        # allorderdata_avg_new = copy.copy(allorderdata_avg)
        #计算每个零售户的分数
        allorderdata_cv_new['BRAND_SCORE'] = (allorderdata_cv_new['R'] * allorderdata_cv_new['R_W'] + allorderdata_cv_new['F'] * allorderdata_cv_new['F_W'] \
                                         + allorderdata_cv_new['M'] * allorderdata_cv_new['M_W'])*100
        allorderdata_avg_new['BRAND_SCORE'] = (allorderdata_avg_new['R'] * allorderdata_avg_new['R_W'] + allorderdata_avg_new['F'] * allorderdata_avg_new['F_W'] \
                                         + allorderdata_avg_new['M'] * allorderdata_avg_new['M_W'])*100

        allorderdata_cv_new = pd.merge(allorderdata_cv_new,bar_score_cv,how= 'left', on='cust_code')
        allorderdata_cv_new = pd.merge(allorderdata_cv_new,C,how= 'left', on='cust_code')
        allorderdata_avg_new = pd.merge(allorderdata_avg_new, bar_score_avg, how='left', on='cust_code')
        allorderdata_avg_new = pd.merge(allorderdata_avg_new, D, how='left', on='cust_code')


        allorderdata_cv_new['ALL_BAR_SCORE_QTY_RANK']= allorderdata_cv_new['ALL_BAR_SCORE_QTY'].rank(ascending=False, method='first')
        allorderdata_avg_new['ALL_BAR_SCORE_QTY_RANK'] = allorderdata_avg_new['ALL_BAR_SCORE_QTY'].rank(ascending=False, method='first')
        allorderdata_cv_new['ALL_BAR_SCORE_AMT_RANK'] = allorderdata_cv_new['ALL_BAR_SCORE_AMT'].rank(ascending=False,method='first')
        allorderdata_avg_new['ALL_BAR_SCORE_AMT_RANK'] = allorderdata_avg_new['ALL_BAR_SCORE_AMT'].rank(ascending=False,method='first')
        allorderdata_cv_new['BRAND_SCORE_RANK'] = allorderdata_cv_new['BRAND_SCORE'].rank(ascending=False, method='first')
        allorderdata_avg_new['BRAND_SCORE_RANK'] = allorderdata_avg_new['BRAND_SCORE'].rank(ascending=False, method='first')

        s_sql1, sql_tab1=data_to_db().load_and_createsql(allorderdata_avg_new,self.para.rfm_score_avg,self.para.rfm_score_cv_struct)

        # print(allorderdata_avg_new.columns.tolist())

        count1 = 0
        sql1 = ''
        for sql_tab_i1 in sql_tab1:
            sql1 = sql1 + sql_tab_i1
            count1 = count1 + 1
            if count1 % 4000 == 0:
                i_sql = s_sql1 + sql1.rsplit(',', 1)[0]
                sql1 =''
                self.con.select(i_sql)
                logger.get_logger().info('插入数据%d条' % count1)

        i_sql = s_sql1 + sql1.rsplit(',', 1)[0]
        self.con.select(i_sql)
        logger.get_logger().info('插入数据%d条' % count1)

        s_sql, sql_tab = data_to_db().load_and_createsql(allorderdata_cv_new, self.para.rfm_score_cv,self.para.rfm_score_cv_struct)

        count = 0
        sql = ''
        for sql_tab_i in sql_tab:
            sql = sql + sql_tab_i
            count = count + 1
            if count % 4000 == 0:
                ins_sql = s_sql + sql.rsplit(',', 1)[0]
                sql=''
                self.con.select(ins_sql)
                logger.get_logger().info('插入数据%d条' % count)

        ins_sql = s_sql + sql.rsplit(',', 1)[0]
        self.con.select(ins_sql)
        logger.get_logger().info('插入数据%d条' % count)

        logger.get_logger().info('品牌计算结束...')

    #####################################################################
    ##计算每个规格的得分情况
    #####################################################################
    def get_rtl_bar_score(self, allorderdata, current_week, w_customize=[1 / 3, 1 / 3, 1 / 3]):
        # current_week = int(current_week)
        bar_code_all = allorderdata[['bar_code']]
        # 去重
        bar_code_dup = bar_code_all.drop_duplicates(subset=['bar_code'], inplace=False)
        logger.get_logger().info('需要处理%d个规格' % len(bar_code_dup))

        sale_all,money_df  = self.datafunc.get_brand_sale(allorderdata)
        logger.get_logger().info('已获取销量信息')
        bar_score_cv_dict = {}
        bar_score_avg_dict = {}

        amt_score_cv = {}
        amt_score_avg = {}
        for bar_index,bar_v_index in enumerate(bar_code_dup.index):

            each_bar = bar_code_dup.loc[bar_v_index,'bar_code']
            barorderdata = allorderdata[allorderdata['bar_code']== each_bar]
            logger.get_logger().info('现在在处理bar_code:'+each_bar)
            # 获取R、F、M、X
            barorderdata = self.datafunc.get_rtl_r_f_m(barorderdata, current_week)
            # print(barorderdata)
            # 获取变异系数权重
            new_barorderdata = copy.copy(barorderdata)
            # print(new_barorderdata)
            barorderdata_cv = self.datafunc.cv_weight(new_barorderdata)

            # 数据标准化
            barorderdata_cv = self.datafunc.max_min_Standar(barorderdata_cv)
            barorderdata    = self.datafunc.max_min_Standar(barorderdata)

            # 自定义权重数据
            barorderdata_avg = barorderdata
            barorderdata_avg['R_W'] = w_customize[0]
            barorderdata_avg['F_W'] = w_customize[1]
            barorderdata_avg['M_W'] = w_customize[2]

            #销量权重
            sale_dt= pd.merge(barorderdata,sale_all,how= 'left',on=['cust_code'])
            money_dt = pd.merge(barorderdata,money_df,how= 'left',on=['cust_code'])
            # print(sale_dt)
            sale_dt['QTY_W'] = sale_dt['X_x']/sale_dt['X_y']
            sale_dt['AMT_W'] = money_dt['M_REGION'] / money_dt['M_NUM']
            # print(sale_dt['X_x'])
            # print(sale_dt['X_y'])
            # print(sale_dt['BAR_W'])
            barorderdata_avg = pd.merge(barorderdata_avg,sale_dt.loc[:,['cust_code','QTY_W','AMT_W']],how= 'left',on=['cust_code'])
            barorderdata_cv = pd.merge(barorderdata_cv,sale_dt.loc[:,['cust_code','QTY_W','AMT_W']],how= 'left',on=['cust_code'])


            # 计算每个零售户的分数
            barorderdata_cv['RFM_SCORE'] = (barorderdata_cv['R'] * barorderdata_cv['R_W'] + barorderdata_cv['F'] * \
                                             barorderdata_cv['F_W'] + barorderdata_cv['M'] * barorderdata_cv['M_W'])*100
            barorderdata_avg['RFM_SCORE'] = (barorderdata_avg['R'] * barorderdata_avg['R_W'] + barorderdata_avg['F'] * \
                                              barorderdata_avg['F_W'] + barorderdata_avg['M'] * barorderdata_avg['M_W'])*100

            barorderdata_cv['BAR_SCORE_QTY'] = barorderdata_cv['RFM_SCORE'] * barorderdata_cv['QTY_W']
            barorderdata_avg['BAR_SCORE_QTY'] = barorderdata_avg['RFM_SCORE'] * barorderdata_avg['QTY_W']

            barorderdata_cv['BAR_SCORE_AMT'] = barorderdata_cv['RFM_SCORE'] * barorderdata_cv['AMT_W']
            barorderdata_avg['BAR_SCORE_AMT'] = barorderdata_avg['RFM_SCORE'] * barorderdata_avg['AMT_W']

            # print(barorderdata_cv.columns.tolist())
            s_sql,sql_tab = data_to_db().load_and_createsql(barorderdata_cv, self.para.rfm_res_tb,self.para.fm_res_tb_struct)

            count = 0
            sql = ''
            for sql_tab_i in sql_tab:
                sql = sql+sql_tab_i
                count =count+1
                if count % 4000 ==0:
                    i_sql = s_sql + sql.rsplit(',',1)[0]
                    sql=''
                    self.con.select(i_sql)
                    logger.get_logger().info('插入数据%d条'%count)

            i_sql = s_sql + sql.rsplit(',', 1)[0]
            self.con.select(i_sql)
            logger.get_logger().info('插入数据%d条' % count)



            #保存每个零售户每个bar_code的加权得分，保存为字典格式
            dict_cv = dict([(i, a) for i, a in zip(barorderdata_cv.cust_code, barorderdata_cv.BAR_SCORE_QTY)])
            dict_avg = dict([(i, a) for i, a in zip(barorderdata_avg.cust_code, barorderdata_avg.BAR_SCORE_QTY)])

            dict_cv_1 = dict([(i, a) for i, a in zip(barorderdata_cv.cust_code, barorderdata_cv.BAR_SCORE_AMT)])
            dict_avg_1 = dict([(i, a) for i, a in zip(barorderdata_avg.cust_code, barorderdata_avg.BAR_SCORE_AMT)])

            bar_score_cv_dict = self.dict_add(bar_score_cv_dict,dict_cv)
            bar_score_avg_dict = self.dict_add(bar_score_avg_dict,dict_avg)

            amt_score_cv = self.dict_add(amt_score_cv, dict_cv_1)
            amt_score_avg = self.dict_add(amt_score_avg, dict_avg_1)

        bar_score_cv = pd.DataFrame.from_dict(bar_score_cv_dict, orient='index', columns=['ALL_BAR_SCORE_QTY'])
        bar_score_cv = bar_score_cv.reset_index().rename(columns={'index': 'cust_code'})

        bar_score_avg = pd.DataFrame.from_dict(bar_score_avg_dict, orient='index', columns=['ALL_BAR_SCORE_QTY'])
        bar_score_avg = bar_score_avg.reset_index().rename(columns={'index': 'cust_code'})

        bar_score_cv1 = pd.DataFrame.from_dict(amt_score_cv, orient='index', columns=['ALL_BAR_SCORE_AMT'])
        bar_score_cv1 = bar_score_cv1.reset_index().rename(columns={'index': 'cust_code'})

        bar_score_avg1 = pd.DataFrame.from_dict(amt_score_avg, orient='index', columns=['ALL_BAR_SCORE_AMT'])
        bar_score_avg1 = bar_score_avg1.reset_index().rename(columns={'index': 'cust_code'})

        logger.get_logger().info('规格计算结束...')

        return bar_score_cv,bar_score_avg,bar_score_cv1,bar_score_avg1

    def dict_add(self,dict1,dict2):
        for key,value in dict1.items():
            if key in dict2.keys():
                dict2[key] = dict2[key]+value
            else:
                dict2[key] = value
        return dict2



























