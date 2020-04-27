import xlsxwriter
import pandas as pd
import numpy as np
from numpy import *
def loadDate(fileName):  #加载数据文件
    fr=open(fileName)
    dataMat=[]
    for line in fr.readlines():
        curLine=line.strip().split("\t")
        lineArr=[]
        lineArr.append(float(curLine[0]))
        lineArr.append(float(curLine[1]))
        lineArr.append(float(curLine[2]))
        dataMat.append(lineArr)
    return dataMat
def normalize(dataMat):
    dataMat=np.array(dataMat)
    [m,n]=dataMat.shape
    for i in range(n):
        dataMat[:,i] =(dataMat[:,i]-min(dataMat[:,i]))/(max(dataMat[:,i])-min(dataMat[:,i]))
    return dataMat
def init_grid(dataMat): #初始化第二层网格
    dataMat=np.array(dataMat)
    [m,n] = dataMat.shape
    #[m, n] = shape(self.dataMat)
    k=0 #构建低二层网络模型
    #数据集的维度即网格的维度，分类的个数即网格的行数
    grid=mat(zeros((4*4,n)))
    for i in range(4):
        for j in range(4):
            grid[k,:]=[i,j]
            k+=1
    return grid
a=loadDate(r'C:\Users\Tangt\Desktop\13.txt')
b=normalize(a)

#print(b.shape)


[m,n] = 4,4
#[m, n] = shape(self.dataMat)
k=0 #构建低二层网络模型
#数据集的维度即网格的维度，分类的个数即网格的行数

w=random.rand(3,4*4)
aaa=b[1,:]

def distEclud(matA,matB):
    if len(matA.shape)==1:
        ma=1
    else:
        ma,mb=matA.shape
    mb, nb = matB.shape
    #ma, na = shape(matA);
    #mb, nb = shape(matB);
    rtnmat = zeros((1, nb))
    for j in range(w.shape[1]):
        rtnmat[0,j]=linalg.norm(aaa[0:]-w[:,j].T)

    return rtnmat
a=distEclud(aaa,w)
b=distEclud(aaa,w).argmin()
d1=ceil(b/4)  #计算此节点在第二层矩阵中的位置
d2=mod(b,4)
print(a)
print(b)
print(d1,d2)
# nb=w.shape[1]
# print(nb)
# print(aaa[0:])
# print(w[:,1])
# print(linalg.norm(aaa[0:]-w[:,1].T))
# rtnmat = zeros((1, nb))
# for j in range(w.shape[1]):
#     rtnmat[0,j]=linalg.norm(aaa[0:]-w[:,j].T)
