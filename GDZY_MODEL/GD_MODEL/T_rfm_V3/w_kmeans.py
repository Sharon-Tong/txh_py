# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/20 10:42
project: 加权K-means
'''

from numpy import *
class MyKmeans():
    '''

    '''

    #计算欧式距离
    def distEclud(self,vecA,vecB):

        return sqrt(sum(power(vecA - vecB, 2)))  # 求两个向量之间的距离

    # 构建聚簇中心，初始簇心：选择彼此距离尽可能远的K个点
    def randCent(self,dataSet,k):
        n = shape(dataSet)[1]
        centroids = mat(zeros((k, n)))  # 每个质心有n个坐标值，总共要k个质心
        for j in range(n):
            minJ = min(dataSet[:, j])
            maxJ = max(dataSet[:, j])
            rangeJ = float(maxJ - minJ)
            centroids[:, j] = minJ + rangeJ * random.rand(k, 1)
        return centroids

