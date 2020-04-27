#-*- coding:utf-8 -*-
#!/usr/bin/python
#from scipy.integrate import odeint
import numpy as np
import pandas   as pd
from  ANALYZE.GDZY_REPORT.LOG.MyLog import logger
#from matplotlib import pyplot as pl


#计算基尼系数
def Gini(data,type=0):
    # 计算数组累计值,从 0 开始
    wealths = data.iloc[:,1]
    if  type==0:
        cum_wealths = np.cumsum(sorted(np.append(wealths, 0)))
        logger.get_logger().info("未剔除零售户订单数为0的Gini系数计算")
        logger.get_logger().info("计算的数据共有:" + str(len(cum_wealths)) + "条")
    if  type==1:
        cum_wealths=    np.cumsum(sorted(np.append(wealths[wealths!=0],0)))
        logger.get_logger().info("剔除零售户订单数为0的Gini系数计算")
        logger.get_logger().info("计算的数据共有:"+str(len(cum_wealths))+"条")
    # 取最后一个，也就是原数组的和
    sum_wealths = cum_wealths[-1]
    # 人数的累积占比
    xarray = np.array(range(0, len(cum_wealths))) / np.float(len(cum_wealths) - 1)

    # 均衡收入曲线
    upper = xarray
    # 收入累积占比
    yarray = cum_wealths / sum_wealths
    # 绘制基尼系数对应的洛伦兹曲线
    # ax.plot(xarray, yarray)
    # ax.plot(xarray, upper)
    # ax.set_xlabel(u'人数累积占比')
    # ax.set_ylabel(u'收入累积占比')
    # pl.show()
    # 计算曲线下面积的通用方法
    B = np.trapz(yarray, x=xarray)
    # 总面积 0.5
    A = 0.5 - B
    G = A / (A + B)
    logger.get_logger().info("计算的基尼系数为:" + str(G))
    return G

data=pd.read_excel(r'C:\Users\Tangt\Desktop\201901.xlsx',engine='xlrd',sheet_name="示例（广州-硬经典）的数据")
data1=pd.read_excel(r'C:\Users\Tangt\Desktop\201901.xlsx',engine='xlrd',sheet_name="扬州-红旗渠的数据")
data2=pd.read_excel(r'C:\Users\Tangt\Desktop\data.xlsx',engine='xlrd')
#Gini(data,1)
#Gini(data1,1)
Gini(data,1)
Gini(data2,0)

