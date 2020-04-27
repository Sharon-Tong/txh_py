import xlsxwriter
import numpy as np
import pandas as pd

##数据导入
file_path=r'C:\Users\Tangt\Desktop\广州同价区.xlsx'
gd_bar_dt1=pd.read_excel(file_path,header=0)

##选取201901-201910的数据
gd_bar_dt=gd_bar_dt1[(gd_bar_dt1.月份>=201901) & (gd_bar_dt1.月份<=201909)]

##取各规格的均值作为竞争力计算的指标
columns=['销量增长率','商业存销比','市场份额','价区销量增长率','零售户数','铺货率','订单满足率','断货率','户均销量','重购率']
gd_bar_dt_avg=[gd_bar_dt[column].groupby(gd_bar_dt['规格编码']).mean() for column in columns]



##数据标准化
def datastandar(data):
    gd_bar_dt_standar=pd.DataFrame()
    min = [bar.min() for bar in data]
    max = [bar.max() for bar in data]
    for i in range(len(columns)):
        if data[i].name=='铺货率':
            gd_bar_dt_standar[i]=(max[i]-data[i])/(max[i]-min[i])
        else:
            gd_bar_dt_standar[i]=(data[i]-min[i])/(max[i]-min[i])
    return gd_bar_dt_standar

gd_bar_dt_standar_1 = datastandar(gd_bar_dt_avg)


##计算标准差和相关系数
std = [np.std(gd_bar_dt_standar_1[i]) for i in gd_bar_dt_standar_1]
corr = gd_bar_dt_standar_1.corr()

##计算critic的指标冲突性
C = [std[i]*sum(1-corr[i]) for i in corr]

##计算各指标的权重
Weight=[i /sum(C) for i in C]

#计算每个规格的市场竞争力
Weight_1=pd.DataFrame(Weight)
bar_zjl=np.dot(gd_bar_dt_standar_1,Weight_1)

##转移矩阵的非对角线
n_len=len(bar_zjl)
zyjz=np.zeros((n_len,n_len))
zyjz_bar=[]

for i in range(n_len):
    a=0
    for j in range(n_len):
        zyjz[i,j]=abs(bar_zjl[j]-bar_zjl[i])
        a+=zyjz[i,j]
    zyjz_bar.append(a)

##修正的转移矩阵非对角线
zyjz_xz = np.zeros((n_len,n_len))
for i in range(n_len):
    for j in range(n_len):
        zyjz_xz[i,j]=zyjz[i,j]/zyjz_bar[i]


##导入拟合后的忠诚度
file_path=r'C:\Users\Tangt\Desktop\忠诚度.xlsx'
gd_bar_zcd=pd.read_excel(file_path,header=0)

#匹配各规格对应的忠诚度
bar_zcd=pd.merge(gd_bar_dt_standar_1,gd_bar_zcd,right_on='bar_code',left_index=True)




filename = (r'C:\Users\Tangt\Desktop\转移矩阵-0218.xlsx')
workbook=xlsxwriter.Workbook(filename,{'nan_inf_to_errors': True})
worksheet=workbook.add_worksheet('转移矩阵')
##最终的转移矩阵
for i in range(n_len):
    for j in range(n_len):
        if j==i:
            zyjz_xz[i,j]=bar_zcd.iloc[i,11]
        else:
            zyjz_xz[i,j]=(1-bar_zcd.iloc[i,11])*zyjz_xz[i,j]
        worksheet.write(i,j+1,zyjz_xz[i][j])
    worksheet.write(i,0,bar_zcd.iloc[i,10])

##预测##########
##初始值
m_0=[i for i in gd_bar_dt_standar_1[2]]
##2步
zyjz_final_2=np.dot(zyjz_xz,zyjz_xz)
zyjz_final_3=np.dot(zyjz_xz,zyjz_xz)
#预测结果
predict_1=np.dot(m_0,zyjz_xz)
predict_2=np.dot(m_0,zyjz_final_2)
predict_3=np.dot(m_0,zyjz_final_3)

worksheet1=workbook.add_worksheet('预测的市场占有率')

for row in range(len(predict_1)):
    worksheet1.write(row,0,bar_zcd.iloc[row,11])
    worksheet1.write(row,1,predict_1[row])
    worksheet1.write(row,2,predict_2[row])
    worksheet1.write(row,3,predict_3[row])

workbook.close()











