from GDZY_REPORT.LOG.MyLog import logger
from GDZY_REPORT.SQL_LINK.DB2_link import MyDB2
from GDZY_REPORT.Sql_param.sqlconfig import db88
from GDZY_REPORT.T_gd_daily_sales_city_bar.daily_param import DailyParam
import time
class SalesConfig():
    ######################################
    #数据库语句：增删改查
    ######################################
    def __init__(self):
        self.startdate  = DailyParam.startdate
        self.enddate    = DailyParam.enddate
        self.last_month = DailyParam.month_id
    pass

    def get_sql(self):

        self.Sql = "select SFMC,a.DSMC ,MAX(CIG_MARKNAME) as PPMC," \
              "(case when c.PPBM='9999' and CIG_PRODUCER='20440001' then '双喜不含好日子'" \
              " when c.PPBM='9999' and CIG_PRODUCER='12440301' then '双喜好日子'" \
              "when c.PPBM='9999' and CIG_PRODUCER='11310001' then '上海红双喜'" \
              "else MAX(CIG_MARKNAME) end ) AS PPZL," \
              "MAX(case when b.C_BRAND='6901028020732' then'椰王1' " \
              " when b.C_BRAND='6901028177184' then'冬虫夏草1' " \
              " when b.C_BRAND='6901028086981' then'冬虫夏草2' " \
              "when b.C_BRAND='6901028008464' then'双喜(花悦)' " \
              "else CIG_TRADEMARK end )as GGMC," \
              "max(C.CIG_TARCONTENT) CIG_TARCONTENT,max(c.CIG_GIRTH)CIG_GIRTH,max(c.CIG_PACKAMOUNT)CIG_PACKAMOUNT," \
              "MAX(CASE WHEN TYPE_NAME='未知' THEN '六类' ELSE TYPE_NAME END)," \
              "MAX(TRANSFER_PRICE_TAX),min(WHOLE_PRICE1),MAX(WHOLE_PRICE),max( RETAILER_PRICE),MAX(C.SCS), " \
              "sum(xssl13),sum(xssl12),sum(xsel13),sum(jxse),sum(xsel12),sum(TQjxse),sum(DRXL),SUM(DYX),SUM(TQDYX),SUM(SYX),SUM(KC),SUM(TQKC) from " \
              "( SELECT ORG_CODE as dqbm,MAX(ORG_SHORTNAME) AS DSMC,ORG_CODE2 as SFBM,MAX(ORG_NAME2) AS SFMC FROM dbread.dim_org WHERE ORG_TYPE='1' AND ORG_GRADE in ('1','2') " \
              "GROUP BY ORG_CODE,ORG_CODE2) as a left join ( select I_ORG as dqbm,C_BRAND ,sum( PRINT_NUM1_Y_A+SELF_SUPP_NUM1_Y_A-RBACK_NUM1_Y_A) *0.2 as xssl13 " \
              ",sum( PRINT_NUM1_Y_AL+SELF_SUPP_NUM1_Y_AL-RBACK_NUM1_Y_AL)*0.2 as xssl12  ,sum(PRINT_SUM_Y_A+SELF_SUPP_SUM_Y_A-RBACK_SUM_Y_A) as xsel13 " \
              ",sum(PRINT_SUM_Y_AL+SELF_SUPP_SUM_Y_AL-RBACK_SUM_Y_AL) as xsel12 ,0 DRXL	,0 DYX,0 TQDYX,0 SYX,0 KC,0 TQKC,0 jxse	,0 TQjxse " \
              "from DB2ADMIN.N_K_TJBS_Y_Q_M_P_D_ALL a ,dbread.dim_bar_public b where  a.C_BRAND=b.bar_code    and d_day='"+self.enddate+"' and BAR_NAME not like '%%出口%%' group by a.I_ORG,a.C_BRAND " \
              "UNION select I_ORG as dqbm,C_BRAND,0  xssl13,0 xssl12,0 xsel13,0 xsel12 ,sum(case when  d_day='"+self.enddate+"'  then PRINT_NUM1+SELF_SUPP_NUM1-RBACK_NUM1 else 0 end )/5 DRXL " \
              ",sum( PRINT_NUM1+SELF_SUPP_NUM1-RBACK_NUM1 )/5 DYX ,sum( PRINT_NUM1_L+SELF_SUPP_NUM1_L-RBACK_NUM1_L)/5  TQDYX,0 SYX ,SUM( case when  d_day='"+self.enddate+"'  THEN TERM_STOCK1 ELSE 0 END)*0.2 KC " \
              ",SUM( case when  d_day='"+ self.enddate +"'  THEN TERM_STOCK1_L ELSE 0 END)*0.2 TQKC ,0 jxse ,0 TQjxse from DB2ADMIN.N_K_TJBS_Y_Q_M_P_D_ALL a ,dbread.dim_bar_public b  " \
              " where  a.C_BRAND=b.bar_code    and d_day>='"+self.startdate+"' and d_day<='"+self.enddate+"' and BAR_NAME not like '%%出口%%' group by a.I_ORG,a.C_BRAND " \
              " UNION select I_ORG as dqbm,C_BRAND,0  xssl13,0 xssl12,0 xsel13,0 xsel12 ,0 DRXL ,0 DYX ,0 TQDYX,SUM(PRINT_NUM1+SELF_SUPP_NUM1-RBACK_NUM1)/5 SYX ,0 KC ,0 TQKC ,0 jxse ,0 TQjxse " \
              " from DB2ADMIN.N_K_TJBS_Y_Q_M_ALL a ,dbread.dim_bar_public b where  a.C_BRAND=b.bar_code    and D_MONTH='"+self.last_month+"' and BAR_NAME not like '%%出口%%' group by a.I_ORG,a.C_BRAND " \
              " UNION select I_ORG as dqbm,b.bar_code C_BRAND,0  xssl13,0 xssl12,0 xsel13,0 xsel12 ,0 DRXL ,0 DYX ,0 TQDYX ,0 SYX ,0 KC ,0 TQKC ,sum(XL*WHOLE_PRICE1/PACKAGE_AMOUNT) jxse " \
               " ,sum(TQxl*WHOLE_PRICE1/PACKAGE_AMOUNT) TQjxse from (select c_brand, I_ORG, sum(PRINT_NUM1_Y_A+SELF_SUPP_NUM1_Y_A-RBACK_NUM1_Y_A) xl,sum(PRINT_NUM1_Y_AL+SELF_SUPP_NUM1_Y_AL-RBACK_NUM1_Y_AL) TQxl,d_day from DB2ADMIN.N_K_TJBS_Y_Q_M_P_D_ALL  group by c_brand, d_day,I_ORG) a , " \
               " (select BAR_CODE,min(WHOLE_PRICE)WHOLE_PRICE1,max(WHOLE_PRICE)WHOLE_PRICE2 from (select BAR_CODE ,WHOLE_PRICE from DBREAD.DIM_PRICE_ADJUSTPRICE20150508  union " \
               " select BAR_CODE ,WHOLE_PRICE from DBREAD.DIM_PRICE_PUBLIC) group by BAR_CODE )  b,dbread.dim_bar_public c " \
               " where   a.c_brand=b.bar_code and  a.c_brand=c.bar_code and d_day='"+self.enddate+"' and BAR_NAME not like '%%出口%%'  group by a.I_ORG,b.bar_code ) as b on  a.dqbm=b.dqbm " \
               " left join(  select a.CGT_CARTON_CODE ,max(CIG_TRADECODE) PPBM,max(a.CIG_MARKNAME) CIG_MARKNAME,max(a.CIG_TRADEMARK) CIG_TRADEMARK,max(a.CIG_TARCONTENT) CIG_TARCONTENT,max(a.CIG_PACKAMOUNT)CIG_PACKAMOUNT,max(a.CIG_GIRTH)CIG_GIRTH,max(b.TYPE_NAME) TYPE_NAME,max(b.WHOLE_PRICE) WHOLE_PRICE,min(WHOLE_PRICE1)WHOLE_PRICE1 , " \
               " max( RETAILER_PRICE)RETAILER_PRICE,  max(b.TRANSFER_PRICE_TAX) TRANSFER_PRICE_TAX,CIG_PRODUCER,max(a.CIG_MARKOWNER) as SCS from  DBREAD.DIM_CIGARETTE a,dbREAD.DIM_PRICE b, " \
               " (select BAR_CODE,min(WHOLE_PRICE)WHOLE_PRICE1 from (select BAR_CODE ,WHOLE_PRICE from DBREAD.DIM_PRICE_ADJUSTPRICE20150508 union select BAR_CODE ,WHOLE_PRICE from DBREAD.DIM_PRICE_PUBLIC)group by BAR_CODE) d "\
               " where a.CGT_CARTON_CODE=b.BAR_CODE  	 and b.bar_code=d.bar_code  group by a.CGT_CARTON_CODE,a.CIG_PRODUCER order by  a.CGT_CARTON_CODE) as c on b.C_BRAND=c.CGT_CARTON_CODE "\
               " group by SFBM,SFMC,a.dqbm,a.DSMC,c.PPBM,b.C_BRAND,CIG_PRODUCER having  sum(xssl13) <>0 or sum(xssl12)<>0 or sum(DRXL)<>0 or sum(KC)<>0 order by SFBM,a.dqbm,c.PPBM,b.C_BRAND "\

    def get_data(self):
        #选择数据库
        db_chose=MyDB2(db88)
        #连接数据库
        db_chose.connect()
        #查询返回的数据
        data=db_chose.select(self.Sql)
        logger.get_logger().info('处理的数据一共有：'+str(len(data)))
        #关闭数据库
        db_chose.close()
        return data
    pass







