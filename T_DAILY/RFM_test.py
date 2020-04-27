import xlsxwriter
import numpy as np
import pandas as pd
rfm_dt=pd.read_excel(r'C:\Users\Tangt\Desktop\广东中烟广州市.xlsx',header=0)

####计算最大的排名
f_rank_max=rfm_dt['f_rank'].max()
zje_rank_max=rfm_dt['zje_rank'].max()
zdx_rank_max=rfm_dt['zdx_rank'].max()

score=np.zeros((len(rfm_dt),3))


# for i in range(len(rfm_dt)):
#     if rfm_dt.loc[i,'zje_rank']<=abs(zje_rank_max*0.05):
#         score[i,0]=5
#     if rfm_dt.loc[i,'zje_rank']>abs(zje_rank_max*0.05) and rfm_dt.loc[i,'zje_rank']<=abs(zje_rank_max*0.1):
#         score[i,0]=4
#     if rfm_dt.loc[i,'zje_rank']>abs(zje_rank_max*0.1) and rfm_dt.loc[i,'zje_rank']<=abs(zje_rank_max*0.2):
#         score[i,0]=3
#     if rfm_dt.loc[i,'zje_rank']>abs(zje_rank_max*0.2) and rfm_dt.loc[i,'zje_rank']<=abs(zje_rank_max*0.3):
#         score[i,0]=2
#     if rfm_dt.loc[i,'zje_rank']>abs(zje_rank_max*0.3):
#         score[i,0]=1
for i in range(len(rfm_dt)):
    if rfm_dt.loc[i,'zje_rank']<=abs(zje_rank_max*0.05):
        score[i,0]=3
    if rfm_dt.loc[i,'zje_rank']>abs(zje_rank_max*0.05) and rfm_dt.loc[i,'zje_rank']<=abs(zje_rank_max*0.2):
        score[i,0]=2
    if rfm_dt.loc[i,'zje_rank']>abs(zje_rank_max*0.2):
        score[i,0]=1

for i in range(len(rfm_dt)):
    if rfm_dt.loc[i,'f_rank']<=abs(f_rank_max*0.15):
        score[i,1]=3
    if rfm_dt.loc[i,'f_rank']>abs(f_rank_max*0.15) and rfm_dt.loc[i,'f_rank']<=abs(f_rank_max*0.3):
        score[i,1]=2
    if rfm_dt.loc[i,'f_rank']>abs(f_rank_max*0.3):
        score[i,1]=1

for i in range(len(rfm_dt)):
    if rfm_dt.loc[i,'zdx_rank']<=abs(zdx_rank_max*0.15):
        score[i,2]=3
    if rfm_dt.loc[i,'zdx_rank']>abs(zdx_rank_max*0.15) and rfm_dt.loc[i,'zdx_rank']<=abs(zdx_rank_max*0.3):
        score[i,2]=2
    if rfm_dt.loc[i,'zdx_rank']>abs(zdx_rank_max*0.3):
        score[i,2]=1

# for i in range(len(rfm_dt)):
#     if rfm_dt.loc[i,'zdx_rank']<=abs(zdx_rank_max*0.1):
#         score[i,2]=5
#     if rfm_dt.loc[i,'zdx_rank']>abs(zdx_rank_max*0.1) and rfm_dt.loc[i,'zdx_rank']<=abs(zdx_rank_max*0.2):
#         score[i,2]=4
#     if rfm_dt.loc[i,'zdx_rank']>abs(zdx_rank_max*0.2) and rfm_dt.loc[i,'zdx_rank']<=abs(zdx_rank_max*0.3):
#         score[i,2]=3
#     if rfm_dt.loc[i,'zdx_rank']>abs(zdx_rank_max*0.3) and rfm_dt.loc[i,'zdx_rank']<=abs(zdx_rank_max*0.5):
#         score[i,2]=2
#     if rfm_dt.loc[i,'zdx_rank']>abs(zdx_rank_max*0.5):
#         score[i,2]=1


# m_des=['','m低','m较低','m中','m高','m较高']
# f_desc=['','f低','f较低','f中','f高','f较高']
# r_desc=['','r低','r较低','r中','r高','r较高']

m_des=['','m低','m中','m高']
f_desc=['','f低','f中','f高']
r_desc=['','r低','r中','r高']

filename = (r'C:\Users\Tangt\Desktop\rfm_test2.xlsx')
workbook=xlsxwriter.Workbook(filename,{'nan_inf_to_errors': True})
worksheet=workbook.add_worksheet('test')
for i in range(len(rfm_dt)):
    for j in range(len(rfm_dt.columns)):
        worksheet.write(i,j,rfm_dt.iloc[i,j])
    for k in range(3):
        worksheet.write(i,k+len(rfm_dt.columns),score[i,k])
    worksheet.write(i,len(rfm_dt.columns)+4,(score[i,0]*0.5+score[i,1]*0.3+score[i,2]*0.2))
    worksheet.write(i,len(rfm_dt.columns)+5,(str(m_des[int(score[i,0])]) + str(f_desc[int(score[i,1])]) + str(r_desc[int(score[i,2])])))

workbook.close()







