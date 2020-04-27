# -*- coding: utf-8 -*-

'''

 Time : 2020/3/30 20:44
 Author : SharonTong

'''
from GDZY_REPORT.LOG.MyLog import logger
from GDZY_REPORT.SQL_LINK.DB2_link import MyDB2
from GDZY_REPORT.Sql_param.sqlconfig import db84
#from GDZY_REPORT.T_gd_weekly_sales.a_weekly_param import DailyParam
import time
class SalesConfig():
    ######################################
    #数据库语句：增删改查
    ######################################
    def __init__(self,DailyParam):
        self.startyear1 = DailyParam.db_startyear1
        self.startyear2 = DailyParam.db_startyear2
        self.enddate1   = DailyParam.db_enddate1
        self.enddate2   = DailyParam.db_enddate2
        self.year1      = DailyParam.db_year1
        self.year2      = DailyParam.db_year2
    pass

    def get_sql(self):

        self.Sql = "select MAX(ORG_NAME2) ORG_NAME2, MAX(ORG_NAME) ORG_NAME,max(PPMC) PPMC," \
               "case when CIG_PRODUCER='20440001' and PPDM='9999' then '双喜不含好日子'" \
               " when CIG_PRODUCER='12440301' and PPDM='9999' then '双喜好日子'" \
               " when CIG_PRODUCER='11310001' and PPDM='9999' then '上海红双喜'" \
               " else (PPMC) end as PPZL,max(case when JYJM='6901028177191'then '冬虫夏草1'" \
               " when JYJM='6901028020343'then '椰王1'" \
               " when JYJM='6901028008518'then '双喜(花悦)' else GGMC end),  max(GYMZ) GYMZ," \
               " max(JL) JL,CIG_TARCONTENT,max(CIG_GIRTH)CIG_GIRTH,max(CIG_PACKAMOUNT)CIG_PACKAMOUNT,TRANSFER_PRICE_TAX,RETAILER_PRICE,WHOLE_PRICE1,WHOLE_PRICE2," \
               " sum(xylsbn18)/5  xylsbn18,sum(xylsbn188)/5 xylsbn188,sum(xylsbn17)/5  xylsbn17, sum(xylsbn177)/5 xylsbn177," \
               " SUM(xylsbn18*WHOLE_PRICE2/CIG_PACKAMOUNT) XSE18,SUM(xylsbn18*WHOLE_PRICE1/CIG_PACKAMOUNT) JXSE18," \
               " SUM(xylsbn188*WHOLE_PRICE2/CIG_PACKAMOUNT) XSE188,SUM(xylsbn188*WHOLE_PRICE1/CIG_PACKAMOUNT) JXSE188," \
               " SUM(xylsbn17*WHOLE_PRICE2/CIG_PACKAMOUNT) XSE17,SUM(xylsbn17*WHOLE_PRICE1/CIG_PACKAMOUNT) JXSE17," \
               " SUM(xylsbn177*WHOLE_PRICE2/CIG_PACKAMOUNT) XSE177,SUM(xylsbn177*WHOLE_PRICE1/CIG_PACKAMOUNT) JXSE177," \
               " sum(QNxyl18)/5 QNxyl18,sum(QNxyl17)/5 QNxyl17,SUM(QNxyl18*WHOLE_PRICE2/CIG_PACKAMOUNT) QNXSE18,SUM(QNxyl18*WHOLE_PRICE1/CIG_PACKAMOUNT) JQNXSE18," \
               " SUM(QNxyl17*WHOLE_PRICE2/CIG_PACKAMOUNT) QNXSE17,SUM(QNxyl17*WHOLE_PRICE1/CIG_PACKAMOUNT) JQNXSE17,SUM(QNxyl17*TRANSFER_PRICE_TAX/CIG_PACKAMOUNT) GYXSE17," \
                " SUM(QNxyl18*TRANSFER_PRICE_TAX/CIG_PACKAMOUNT) GYXSE18, sum(hetong18)/5,sum( hetong188)/5 from " \
                " ( select CGT_CARTON_CODE JYTM,barcode_03 JYJM,MAX(CIG_TRADEMARK) GGMC,CIG_TRADECODE PPDM,MAX(CIG_MARKNAME) PPMC,A.CIG_PRODUCER, MAX(CIG_MARKOWNER) GYMZ,"\
                "max(CIG_GIRTH)CIG_GIRTH,TYPE_CODE,MAX(TYPE_NAME) JL,CIG_TARCONTENT, WHOLE_PRICE1, WHOLE_PRICE2,TRANSFER_PRICE_TAX,RETAILER_PRICE,max(CIG_PACKAMOUNT)CIG_PACKAMOUNT" \
                " from DBREAD.DIM_CIGARETTE_88  A,DBREAD.DIM_PRICE B,dbread.dim_barcode03 C, (select BAR_CODE,min(WHOLE_PRICE)WHOLE_PRICE1,max(WHOLE_PRICE) WHOLE_PRICE2" \
                " from (select BAR_CODE ,WHOLE_PRICE from DB2ADMIN.N_DIM_PRICE_ADJUSTPRICE20150508 union select BAR_CODE ,WHOLE_PRICE from DBREAD.DIM_PRICE_PUBLIC)group by BAR_CODE) E" \
               " where CIG_BARCARRIER='02' and A.CGT_CARTON_CODE=B.BAR_CODE and CGT_CARTON_CODE=barcode_02 and A.CIG_PRODUCER=C.CIG_PRODUCER AND A.CGT_CARTON_CODE=E.BAR_CODE  and CIG_TRADEMARK not like '%%出口%%'" \
               " group by CGT_CARTON_CODE,barcode_03,CIG_TRADECODE,A.CIG_PRODUCER,CIG_TARCONTENT,WHOLE_PRICE1, WHOLE_PRICE2,TYPE_CODE,TRANSFER_PRICE_TAX,RETAILER_PRICE ) A Left join" \
              "( select ORG_CODE2,MAX(ORG_NAME2) ORG_NAME2, ORG_CODE,MAX(ORG_NAME) ORG_NAME,GGJM, sum(xylsbn18)  xylsbn18, sum(xylsbn188) xylsbn188,sum(QNxyl18)QNxyl18,sum(xylsbn17)  xylsbn17,"\
               " sum(xylsbn177)xylsbn177,sum(QNxyl17) QNxyl17 ,sum(hetong18)hetong18,sum(hetong188)hetong188 from (select ORG_CODE,MAX(ORG_NAME) ORG_NAME,ORG_CODE2,MAX(ORG_NAME2) ORG_NAME2 " \
               " from DBREAD.DIM_ORG_88  where  ORG_TYPE='1'  and ORG_GRADE in ('2','1') group by ORG_CODE,ORG_CODE2 ) A left join " \
               " (select case  when char(A.BMEM_id)='99310101' then '11310001' "\
                         " when char(A.BMEM_id)='11445205' then '11445201' "\
               " when char(A.BMEM_id)='11440503' then '11440501' "\
               " when char(A.BMEM_id)='11440705' then '11440701' " \
               " when char(A.BMEM_id)='11445302' then '11445301' " \
               " else char(A.BMEM_id) end  as DSBM,substr(product_id,1,13) GGJM" \
               " ,sum(case when A.DELI_DATE in ('"+self.enddate1+"')then  QUANTITY else 0 end ) xylsbn18 ,sum(case when A.DELI_DATE in ('"+self.enddate2+"')then  QUANTITY else 0 end ) xylsbn188" \
                ",sum(case when A.DELI_DATE in ('"+self.startyear1+"')then  QUANTITY else 0 end ) xylsbn17 ,sum(case when A.DELI_DATE in ('"+self.startyear2+"')then  QUANTITY else 0 end ) xylsbn177" \
                ",sum(case when A.DELI_DATE in ('"+self.startyear1+"','"+self.startyear2+"')then  QUANTITY else 0 end ) QNxyl17 ,sum(case when A.DELI_DATE in ('"+self.enddate1+"','"+self.enddate2+"')then  QUANTITY else 0 end ) QNxyl18" \
                ",0 hetong18,0 hetong188 from DBREAD.N_BAS_D_ELEC_TRADE a,DBREAD.N_BAS_D_ELEC_TRADE_PRODUCT b where A.DELI_DATE in( '"+self.startyear1+"','"+self.startyear2+"','"+self.enddate1+"','"+self.enddate2+"') and "\
                " a.order_id=b.order_id   and del_date is null group by A.BMEM_id,product_id union all select case  when char(A.BMEM_id)='99310101' then '11310001'" \
                " else char(A.BMEM_id) end  as DSBM,substr(product_id,1,13) GGJM ,0 xylsbn18,0 xylsbn188,0 xylsbn17,0 xylsbn177,0 QNxyl17,0 QNxyl18" \
                " ,sum( case when A.DELI_DATE in('"+self.enddate1+"') then  QUANTITY   else  0 end ) hetong18 ,sum( case when A.DELI_DATE in('"+self.enddate2+"') then  QUANTITY   else  0 end ) hetong188" \
                " from DBREAD.N_BAS_D_ELEC_TRADE_ORDER a,DBREAD.N_BAS_D_ELEC_TRADE_ORDER_PRODUCT b where A.DELI_DATE in ('"+self.enddate1+"','"+self.enddate2+"')and a.order_id=b.order_id"\
                " and DEL_DATE IS NULL and A.KIND IN ('工商','手卷雪茄') group by A.BMEM_id,product_id) B on A.ORG_CODE=B.DSBM group by ORG_CODE2,ORG_CODE,GGJM )  B on A.JYJM=B.GGJM" \
                " group by ORG_CODE2, ORG_CODE, PPDM, PPMC,JYJM,TYPE_CODE,CIG_TARCONTENT,TRANSFER_PRICE_TAX,RETAILER_PRICE,WHOLE_PRICE1,WHOLE_PRICE2,CIG_PRODUCER order by ORG_CODE2, ORG_CODE,PPDM,JYJM,TYPE_CODE with ur;"

    def get_data(self):
        #选择数据库
        db_chose=MyDB2(db84)
        #连接数据库
        db_chose.connect()
        #查询返回的数据
        data=db_chose.select(self.Sql)
        logger.get_logger().info('处理的数据一共有：'+str(len(data)))
        #关闭数据库
        db_chose.close()
        return data
    pass










