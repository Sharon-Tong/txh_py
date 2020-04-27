# -*- coding: utf-8 -*-

'''

 Time : 2020/3/30 20:44
 Author : SharonTong

'''
from GDZY_REPORT.LOG.MyLog import logger
from GDZY_REPORT.SQL_LINK.DB2_link import MyDB2
from GDZY_REPORT.Sql_param.sqlconfig import db221
from GDZY_REPORT.excel.nocolor_excel import To_excel
from GDZY_REPORT.excel.monthly_excel import To_excel_shuangxi
import time
from GDZY_REPORT.T_gd_z_monthly_sale.a_monthly_param import DailyParam

class SalesConfig():
    ######################################
    #数据库语句：增删改查
    ######################################
    def __init__(self,DailyParam):
        self.month_id    = DailyParam.last_month
        self.strat_month = DailyParam.start_month_sale
        self.end_month   = DailyParam.end_month_sale

    pass

    def get_sql(self):
        #双喜各省份五率当月数据情况

        self.Sql1 = "SELECT A.MONTH_ID,MAX(A.PROV_NAME), A.BAR_CODE, MAX(BAR_NAME),MAX(TYPE_NAME),SUM(SALE_NUM),SUM(COVER_RATE),"\
               " DB2ADMIN.F_DIV(SUM(HS),SUM(ZHS),2),SUM(HS), SUM(SALE_NUM_PER_RETAILER), DB2ADMIN.F_DIV(SUM(NUMER),SUM(DSS),2),"\
               " SUM(ZHS),SUM(DSS),SUM(QXS),SUM(CGH) FROM (SELECT MONTH_ID, PROV_CODE,MAX(PROV_NAME)PROV_NAME, A.BAR_CODE,"\
               " MAX(BAR_NAME)BAR_NAME, MAX(TYPE_NAME)TYPE_NAME,SUM(SALE_NUM)/5 SALE_NUM,SUM(COVER_RATE)COVER_RATE, SUM(DISTRIBUTION_RATE)DISTRIBUTION_RATE,"\
               " SUM(RETAILER_NUM) HS, SUM(SALE_NUM_PER_RETAILER)SALE_NUM_PER_RETAILER FROM MQT.FACT_BAR_PRV_5M A, DBREAD.N_DIM_BAR_PUBLIC B,"\
               " (SELECT DISTINCT PROV_CODE,MAX(PROV_NAME)PROV_NAME FROM DBREAD.DIM_ORG_BS_PUBLIC GROUP BY PROV_CODE) C, MQT.DIM_PRICE D "\
               " WHERE A.BAR_CODE=B.BAR_CODE AND A.PRV_CODE=C.PROV_CODE AND BRAND_CODE='9999' AND B.PRODUCER_CODE='20440001'  AND MONTH_ID in ('"+self.month_id+"') AND B.BAR_CODE=D.BAR_CODE"\
               " GROUP BY PROV_CODE ,MONTH_ID,A.BAR_CODE) A LEFT JOIN (SELECT  PROV_CODE,  MAX(PROV_NAME), MONTH_ID, BAR_CODE, COUNT(  DISTINCT CITY_CODE)  NUMER FROM"\
               " (SELECT PROV_CODE, MAX(PROV_NAME) PROV_NAME,A.CITY_CODE, MONTH_ID, A.BAR_CODE,SUM(SALE_NUM) FROM MQT.FACT_BAR_CITY_5M A, "\
               "(SELECT DISTINCT PROV_CODE, CITY_CODE ,MAX(PROV_NAME)PROV_NAME FROM DBREAD.N_DIM_ORG_BS_PUBLIC GROUP BY PROV_CODE, CITY_CODE  ) B, DBREAD.N_DIM_BAR_PUBLIC C"\
               " WHERE  A.CITY_CODE=B.CITY_CODE  AND A.BAR_CODE=C.BAR_CODE  AND MONTH_ID in('"+ self.month_id+"') AND BAR_NAME NOT LIKE '%%出口%%' AND BRAND_CODE='9999' AND "\
               " C.PRODUCER_CODE='20440001' GROUP BY  PROV_CODE, A.CITY_CODE,A.BAR_CODE,MONTH_ID HAVING SUM(SALE_NUM)>0) GROUP BY  PROV_CODE,BAR_CODE,MONTH_ID)  B "\
               " ON A.PROV_CODE=B.PROV_CODE AND A.MONTH_ID=B.MONTH_ID AND  A.BAR_CODE=B.BAR_CODE LEFT JOIN (SELECT  MONTH_ID,PROV_CODE,COUNT(DISTINCT A.CITY_CODE) DSS,"\
               " COUNT(DISTINCT DEPT_CODE)QXS FROM DBREAD.N_FACT_M_DEPT_RTL_SUM A,(SELECT DISTINCT PROV_CODE,CITY_CODE FROM DBREAD.N_DIM_ORG_BS_PUBLIC GROUP BY PROV_CODE,CITY_CODE) B"\
               " WHERE   A.CITY_CODE=B.CITY_CODE AND MONTH_ID in('"+ self.month_id+"') GROUP BY  MONTH_ID,PROV_CODE)C ON A.PROV_CODE=C.PROV_CODE AND A.MONTH_ID=C.MONTH_ID"\
               " LEFT JOIN (SELECT MONTH_ID, PROV_CODE ,COUNT( DISTINCT RETAILER_CODE)ZHS FROM DBREAD.N_FACT_M_RTL_RANK A,( SELECT DISTINCT  PROV_CODE ,CITY_CODE FROM "\
               " DBREAD.N_DIM_ORG_BS_PUBLIC  GROUP BY PROV_CODE , CITY_CODE) B WHERE  MONTH_ID in('"+ self.month_id+"')  AND A.CITY_CODE=B.CITY_CODE GROUP BY MONTH_ID, PROV_CODE"\
               " HAVING SUM(SALE_NUM)>0 ) D ON A.PROV_CODE=D.PROV_CODE AND A.MONTH_ID=D.MONTH_ID LEFT JOIN (SELECT MONTH_ID, BAR_CODE, PROV_CODE, PROV_NAME, SUM(CGH) CGH,"\
               " SUM(GJH) GJH, DB2ADMIN.F_DIV(SUM(CGH),SUM(GJH),2) CGL FROM (SELECT MONTH_ID, CITY_CODE, BAR_CODE, SUM(CASE WHEN ORDER_NUM_TYPE IN('2','3','9') THEN RETAILER_NUM "\
               " ELSE 0 END) CGH, SUM(CASE WHEN ORDER_NUM_TYPE IN('1','2','3','9') THEN RETAILER_NUM ELSE 0 END) GJH FROM MQT.RPT_TIMES_BAR_M_ALL WHERE BAR_CODE IN(select CGT_CARTON_CODE"\
               " from DBREAD.N_DIM_CIGARETTE where CIG_TRADECODE='9999' and CIG_PRODUCER='20440001' group by CGT_CARTON_CODE) AND MONTH_ID in('"+ self.month_id+"') GROUP BY MONTH_ID, CITY_CODE,"\
               " BAR_CODE) A, (SELECT PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME FROM DBREAD.N_DIM_ORG_BS_PUBLIC GROUP BY PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME) B WHERE "\
               " A.CITY_CODE=B.CITY_CODE GROUP BY MONTH_ID, BAR_CODE, PROV_CODE, PROV_NAME HAVING SUM(CGH)>0) E ON A.PROV_CODE=E.PROV_CODE AND A.MONTH_ID=E.MONTH_ID AND "\
               " A.BAR_CODE=E.BAR_CODE GROUP BY A.PROV_CODE,A.MONTH_ID,A.BAR_CODE ORDER BY A.PROV_CODE,A.MONTH_ID,A.BAR_CODE WITH UR;"

        # 查询双喜各省各地市五率当月数据情况
        self.Sql2="SELECT A.MONTH_ID,MAX(A.PROV_NAME),MAX(A.CITY_NAME)CITY_NAME, A.BAR_CODE, MAX(BAR_NAME), MAX(TYPE_NAME),SUM(XL), SUM(COVER_RATE),"\
                " DB2ADMIN.F_DIV(SUM(HS),SUM(ZHS),2),SUM(HS), SUM(SALE_NUM_PER_RETAILER),SUM(ZHS),SUM(QXS),SUM(CGH) FROM (SELECT MONTH_ID, "\
                " PROV_CODE,MAX(PROV_NAME)PROV_NAME,A.CITY_CODE,MAX(CITY_NAME)CITY_NAME, A.BAR_CODE, MAX(BAR_NAME)BAR_NAME, MAX(TYPE_NAME)TYPE_NAME,"\
                " SUM(SALE_NUM)/5 XL, SUM(COVER_RATE) COVER_RATE, SUM(DISTRIBUTION_RATE)DISTRIBUTION_RATE, SUM(RETAILER_NUM)HS, "\
                " SUM(SALE_NUM_PER_RETAILER)SALE_NUM_PER_RETAILER FROM MQT.FACT_BAR_CITY_5M A, DBREAD.N_DIM_BAR_PUBLIC B, (SELECT DISTINCT PROV_CODE,"\
                " MAX(PROV_NAME)PROV_NAME,CITY_CODE,MAX(CITY_NAME)CITY_NAME FROM DBREAD.N_DIM_ORG_BS_PUBLIC GROUP BY PROV_CODE,CITY_CODE) C,MQT.DIM_PRICE D"\
                " WHERE A.BAR_CODE=B.BAR_CODE AND A.CITY_CODE=C.CITY_CODE AND BRAND_CODE='9999' AND B.PRODUCER_CODE='20440001' AND MONTH_ID in('"+ self.month_id+"') AND "\
                " B.BAR_CODE=D.BAR_CODE AND SALE_NUM>0 GROUP BY PROV_CODE ,A.CITY_CODE,MONTH_ID,A.BAR_CODE )A LEFT JOIN (SELECT MONTH_ID,CITY_CODE,COUNT(DISTINCT DEPT_CODE)QXS"\
                " FROM DBREAD.N_FACT_M_DEPT_RTL_SUM WHERE MONTH_ID in('"+ self.month_id+"') GROUP BY  MONTH_ID,CITY_CODE)B ON A.CITY_CODE=B.CITY_CODE AND A.MONTH_ID=B.MONTH_ID"\
                " LEFT JOIN (SELECT MONTH_ID, CITY_CODE,COUNT( DISTINCT RETAILER_CODE) ZHS FROM DBREAD.N_FACT_M_RTL_RANK WHERE  MONTH_ID in('"+ self.month_id+"') GROUP BY"\
                " MONTH_ID, CITY_CODE HAVING SUM(SALE_NUM)>0) C ON  A.CITY_CODE=C.CITY_CODE  AND A.MONTH_ID=C.MONTH_ID LEFT JOIN (SELECT MONTH_ID, BAR_CODE, "\
                " PROV_CODE, PROV_NAME, A.CITY_CODE, CITY_NAME, SUM(CGH) CGH, SUM(GJH) GJH, DB2ADMIN.F_DIV(SUM(CGH),SUM(GJH),2) CGL FROM (SELECT MONTH_ID, CITY_CODE, BAR_CODE,"\
                " SUM(CASE WHEN ORDER_NUM_TYPE IN('2','3','9') THEN RETAILER_NUM ELSE 0 END) CGH, SUM(CASE WHEN ORDER_NUM_TYPE IN('1','2','3','9') THEN RETAILER_NUM ELSE 0 END) GJH"\
                " FROM MQT.RPT_TIMES_BAR_M_ALL WHERE BAR_CODE IN(select CGT_CARTON_CODE from DBREAD.N_DIM_CIGARETTE where CIG_TRADECODE='9999' and CIG_PRODUCER='20440001'"\
                " group by CGT_CARTON_CODE) AND MONTH_ID in('"+ self.month_id+"') GROUP BY MONTH_ID, CITY_CODE, BAR_CODE) A, (SELECT PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME "\
		        " FROM DBREAD.N_DIM_ORG_BS_PUBLIC GROUP BY PROV_CODE, PROV_NAME, CITY_CODE, CITY_NAME) B WHERE A.CITY_CODE=B.CITY_CODE GROUP BY MONTH_ID, BAR_CODE, PROV_CODE,"\
                " PROV_NAME, A.CITY_CODE, CITY_NAME HAVING SUM(CGH)>0) E ON A.CITY_CODE=E.CITY_CODE AND A.MONTH_ID=E.MONTH_ID AND A.BAR_CODE=E.BAR_CODE GROUP BY A.MONTH_ID,"\
                " A.PROV_CODE,A.CITY_CODE, A.BAR_CODE ORDER BY  A.MONTH_ID, A.PROV_CODE,A.CITY_CODE, A.BAR_CODE WITH UR;"

        # 全国各区县双喜销售当年累计数据查询情况
        self.Sql3="select  max(prov_name)prov_name ,max(city_name)city_name, max(dept_name)dept_name,sum(SALE_NUM)/5,"\
             " sum(case when class_code='01' then SALE_NUM else 0 end )/5, sum(case when class_code='02' then SALE_NUM else 0 end )/5, sum(SALE_MONEY)/10000, "\
             " sum(case when class_code='01' then SALE_MONEY else 0 end )/10000, sum(case when class_code='02' then SALE_MONEY else 0 end )/10000 "\
             " from MQT.FACT_BAR_DEPT_5M a, dbread.n_dim_bar_public b, (select distinct prov_code,max(prov_name)prov_name,city_code,max(city_name)city_name ,"\
             " dept_code ,max(dept_name)dept_name from dbread.n_dim_org_bs_public group by prov_code,city_code,dept_code)c, dbread.n_dim_price_public d "\
             " where  a.bar_code=b.bar_code  and b.bar_code=d.bar_code and a.dept_code=c.dept_code and brand_code='9999' and producer_code='20440001' and "\
             " month_id>='"+ self.strat_month+"' and month_id<='"+ self.end_month+"'group by  prov_code,city_code,a.dept_code order by  prov_code,city_code,a.dept_code with ur;"

        # 全国各区县双喜销售当月数据查询情况
        self.Sql4="select  max(prov_name)prov_name ,max(city_name)city_name, max(dept_name)dept_name, sum(SALE_NUM)/5,"\
               " sum(case when class_code='01' then SALE_NUM else 0 end )/5, sum(case when class_code='02' then SALE_NUM else 0 end )/5, sum(SALE_MONEY)/10000,"\
               " sum(case when class_code='01' then SALE_MONEY else 0 end )/10000, sum(case when class_code='02' then SALE_MONEY else 0 end )/10000 "\
               " from MQT.FACT_BAR_DEPT_5M a, dbread.n_dim_bar_public b, (select distinct prov_code,max(prov_name)prov_name,city_code,max(city_name)city_name ,"\
               " dept_code ,max(dept_name)dept_name from dbread.n_dim_org_bs_public group by prov_code,city_code,dept_code)c, dbread.n_dim_price_public d"\
               " where  a.bar_code=b.bar_code  and b.bar_code=d.bar_code and a.dept_code=c.dept_code and brand_code='9999' and producer_code='20440001' and "\
               " month_id='"+ self.end_month+"' group by  prov_code,city_code,a.dept_code order by  prov_code,city_code,a.dept_code with ur;"

    def get_data(self):
        #选择数据库
        db_chose=MyDB2(db221)
        #连接数据库
        db_chose.connect()
        #查询返回的数据
        logger.get_logger().info('开始处理五率表')
        data=db_chose.select(self.Sql1)
        logger.get_logger().info('省份数据一共有：'+str(len(data)))
        data1=db_chose.select(self.Sql2)
        logger.get_logger().info('地市数据一共有：'+str(len(data1)))
        logger.get_logger().info('\n\n\n\n开始写入excel...\n')
        #写excel
        excel = To_excel(DailyParam)
        excel.save_data_2_excel(data,data1)
        logger.get_logger().info('开始处理双喜累计')
        data2=db_chose.select(self.Sql3)
        excel_SX = To_excel_shuangxi(DailyParam)
        excel_SX.save_data_2_excel(data2,DailyParam.filename_lj,DailyParam.key_order_lj)
        logger.get_logger().info('开始处理双喜当月')
        data3=db_chose.select(self.Sql4)
        excel_SX.save_data_2_excel(data3,DailyParam.filename_dy,DailyParam.key_order_dy)
        #关闭数据库
        db_chose.close()
    pass



if __name__== "__main__":
    starttime=time.time()
    a=SalesConfig(DailyParam)
    a.get_sql()
    a.get_data()
    endtime=time.time()
    m,s=divmod(endtime-starttime, 60)
    print("运行时间为：%02d分%02d秒" % (m,s))







