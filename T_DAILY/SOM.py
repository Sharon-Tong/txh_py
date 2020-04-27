from numpy import *
import numpy as np
import matplotlib.pyplot as plt
import string
import xlsxwriter

class Kohonen(object):
    def __init__(self):
        self.lratemax=0.8   #最大学习率-欧式距离
        self.lratemin=0.05  #最小学习率-欧式距离
        self.rmax=5         #最大聚类半径--根据数据集
        self.rmin=0.5       #最小聚类半径--根据数据集
        self.Steps=1000        #迭代次数
        self.lratelist=[]   #学习率收敛曲线
        self.rlist=[]       #学习率半径曲线
        self.w=[]           #权重向量组
        self.M=4            #M*N表示聚类总数
        self.N=4            #M、N表示邻域的参数
        self.dataMat=[]     #外部导入数据集
        self.classLabel=[]  #聚类后的类别标签

    def loadDate(self,fileName):  #加载数据文件
        fr=open(fileName)
        for line in fr.readlines():
            curLine=line.strip().split("\t")
            lineArr=[]
            lineArr.append(float(curLine[0]))
            lineArr.append(float(curLine[1]))
            lineArr.append(float(curLine[2]))
            self.dataMat.append(lineArr)
        self.dataMat=mat(self.dataMat)

    def file2matrix(self,path, delimiter):
        recordlist = []
        fp = open(path)
        content = fp.read()
        fp.close()
        rowlist = content.splitlines()  # 按行转换为一维表
        # 逐行遍历      # 结果按分隔符分割为行向量
        recordlist = [map(eval, row.split(delimiter)) for row in rowlist if row.strip()]
        # 返回转换后的矩阵形式
        self.dataMat = mat(recordlist)

    def normalize(self,dataMat):
        dataMat=np.array(dataMat)
        [m,n]=dataMat.shape
        for i in range(n):
            dataMat[:,i] =(dataMat[:,i]-min(dataMat[:,i]))/(max(dataMat[:,i])-min(dataMat[:,i]))
        return dataMat

    #[m,n]=shape(dataMat)
    #for i in range(n):
    #    dataMat[:,i] =(dataMat[:,i]-min(dataMat[:,i]))/(max(dataMat[:,i])-min(dataMat[:,i]))
    #return dataMat
    def distEclud(self,matA,matB):
        if len(matA.shape)==1:
            ma=1
        else:
            ma,mb=matA.shape
        mb, nb = matB.shape
        #ma, na = shape(matA);
        #mb, nb = shape(matB);
        rtnmat = zeros((1, nb))
        for j in range(nb):
            rtnmat[0,j]=linalg.norm(matA[0:]-matB[:,j].T)
        return  rtnmat
    # def distEclud(self,matA,matB):
    #         ma, na = matA.shape
    #         mb, nb = matB.shape
    #     #ma, na = shape(matA);
    #     #mb, nb = shape(matB);
    #     rtnmat = zeros((ma, nb))
    #     for i in range(ma):
    #         for j in range(nb):
    #             rtnmat[i, j] = linalg.norm(matA[i, :] - matB[:, j].T)
    #     return rtnmat

    def init_grid(self): #初始化第二层网格
        [m,n] = self.dataMat.shape
        #[m, n] = shape(self.dataMat)
        k=0 #构建低二层网络模型
        #数据集的维度即网格的维度，分类的个数即网格的行数
        grid=mat(zeros((self.M*self.N,2)))
        for i in range(self.M):
            for j in range(self.N):
                grid[k,:]=[i,j]
                k+=1
        return grid

    def ratecalc(self,i):
        lrate = self.lratemax - (i + 1.0) * (self.lratemax - self.lratemin) / self.Steps
        r = self.rmax - ((i + 1.0) * (self.rmax - self.rmin)) / self.Steps
        return lrate,r

    #主程序
    def train(self):
        #1.构建输入层网络
        dm,dn=self.dataMat.shape
        #dm,dn=shape(self.dataMat)
        #归一化数据
        normDataSet=self.normalize(self.dataMat)
        #2.初始化第二层分类网络
        grid=self.init_grid()
        #3.随机初始化两层之间的权重向量
        self.w=random.rand(dn,self.M*self.N)
        distM=self.distEclud  #确定距离公式
        #4.迭代求解
        if self.Steps<5*dm:self.Steps=5*dm  #设定最小迭代次数
        for i in range(self.Steps):
            lrate,r=self.ratecalc(i) #1.计算当前迭代次数下的学习率和学习聚类半径
            self.lratelist.append(lrate);self.rlist.append(r)
            #2.随机生成样本索引，并抽取一个样本
            k=random.randint(0,dm)
            mySample=normDataSet[k,:]
            #3.计算最优节点：返回最小距离的索引值
            minIndx=(distM(mySample,self.w)).argmin()
            #4.计算领域
            d1=ceil(minIndx/self.M)  #计算此节点在第二层矩阵中的位置
            d2=mod(minIndx,self.M)
            distMat=distM(mat([d1,d2]),grid.T)
            nodelindx=(distMat<r).nonzero()[1] #获取领域内的所有点
            for j in range(shape(self.w)[1]):
                if sum(nodelindx==j):
                    self.w[:,j]=self.w[:,j]+lrate*(mySample[0]-self.w[:,j])
        #主循环结束
        filename = (r'C:\Users\Tangt\Desktop\rfm_som2.xlsx')
        workbook=xlsxwriter.Workbook(filename,{'nan_inf_to_errors': True})
        worksheet=workbook.add_worksheet('som')
        self.classLabel=[] #分配和存储聚类后的类别标签
        for i in range(dm):
            self.classLabel.append(distM(normDataSet[i,:],self.w).argmin())
            worksheet.write(i,0,distM(normDataSet[i,:],self.w).argmin())
        self.classLabel=mat(self.classLabel)
        workbook.close()


    def showCluster(self,plt): #绘图
        lst=unique(self.classLabel.tolist()[0]) #去重
        i=0
        for cindx in lst:
            myclass = nonzero(self.classLabel==cindx)[1]
            xx=self.dataMat[myclass].copy()
            if i==0: plt.plot(xx[:,0],xx[:,1],'bo')
            elif i==1:plt.plot(xx[:,0],xx[:,1],'rd')
            elif i==2:plt.plot(xx[:,0],xx[:,1],'gD')
            elif i==3:plt.plot(xx[:,0],xx[:,1],'c^')
            elif i==4:plt.plot(xx[:,0],xx[:,1],'b^')
            elif i==5:plt.plot(xx[:,0],xx[:,1],'r^')
            elif i==6:plt.plot(xx[:,0],xx[:,1],'rd')
            elif i==7:plt.plot(xx[:,0],xx[:,1],'yd')
            elif i==8:plt.plot(xx[:,0],xx[:,1],'bd^')
            elif i==9:plt.plot(xx[:,0],xx[:,1],'ro')
            elif i==10:plt.plot(xx[:,0],xx[:,1],'yo')
            elif i==11:plt.plot(xx[:,0],xx[:,1],'go')
            elif i==12:plt.plot(xx[:,0],xx[:,1],'bD')
            i+=1
        plt.show()


if __name__=="__main__":

    SOMNet=Kohonen()
    SOMNet.loadDate(r'C:\Users\Tangt\Desktop\11.txt')
    SOMNet.train()
    SOMNet.showCluster(plt)
