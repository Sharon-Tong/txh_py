# !/usr/bin/env python3
# -*- coding: utf-8 -*-
# Project: BrandAnalyze Analysis
# Author:  yyg
# Created on 2017-10-12
##########################################
from com.ctitc.bigdata.entry.baseentry import BaseEntry
##########################################
# 数据库连接配置信息读取
##########################################
class ImgEntry(BaseEntry):
    CONFIG_FILENM = "img.conf"
    ##########################################
    # 初始化配置文件
    ##########################################
    def __init__(self):
        super().__init__(self.CONFIG_FILENM)
        self.__IMAGE_PATH = self.cf.get("img","IMAGE_PATH")
        self.__IMAGE_WIDTH = self.cf.get("img","IMAGE_WIDTH")
        self.__IMAGE_HEIGHT = self.cf.get("img","IMAGE_HEIGHT")
        self.__IMAGE_CHANNELS = self.cf.get("img","IMAGE_CHANNELS")

        self.__NUM_LABELS = self.cf.get("md","NUM_LABELS")
        self.__CONV1_DEEP = self.cf.get("md","CONV1_DEEP")
        self.__CONV1_SIZE = self.cf.get("md","CONV1_SIZE")
        self.__CONV2_DEEP = self.cf.get("md","CONV2_DEEP")
        self.__CONV2_SIZE = self.cf.get("md","CONV2_SIZE")
        self.__CONV3_DEEP = self.cf.get("md","CONV3_DEEP")
        self.__CONV3_SIZE = self.cf.get("md","CONV3_SIZE")
        self.__CONV4_DEEP = self.cf.get("md","CONV4_DEEP")
        self.__CONV4_SIZE = self.cf.get("md","CONV4_SIZE")
        self.__FC1_SIZE = self.cf.get("md","FC1_SIZE")
        self.__FC2_SIZE = self.cf.get("md","FC2_SIZE")
        self.__TRAINING_STEPS = self.cf.get("md","TRAINING_STEPS")
        self.__BATCH_SIZE = self.cf.get("md","BATCH_SIZE")
        self.__LEARNING_RATE = self.cf.get("md","LEARNING_RATE")
        self.__LEARNING_RATE_DECAY = self.cf.get("md","LEARNING_RATE_DECAY")
        self.__REGULARAZTION_RATE = self.cf.get("md","REGULARAZTION_RATE")
        self.__MOVING_AVERAGE_DECAY = self.cf.get("md","MOVING_AVERAGE_DECAY")
        self.__MODEL_SAVE_PATH = self.cf.get("md","MODEL_SAVE_PATH")
        self.__MODEL_NAME = self.cf.get("md","MODEL_NAME")
    pass
    ##########################################
    # 图片尺寸及存储路径
    ##########################################
    @property
    def IMAGE_PATH(self):
        return self.__IMAGE_PATH
    pass
    @property
    def IMAGE_WIDTH(self):
        return self.__IMAGE_WIDTH
    pass
    @property
    def IMAGE_HEIGHT(self):
        return self.__IMAGE_HEIGHT
    pass
    @property
    def IMAGE_CHANNELS(self):
        return self.__IMAGE_CHANNELS
    pass
    ##########################################
    # 图片分类数量
    ##########################################
    @property
    def NUM_LABELS(self):
        return self.__NUM_LABELS
    pass
    ##########################################
    # 模型参数及存储路径
    ##########################################
    @property
    def CONV1_DEEP(self):
        return self.__CONV1_DEEP
    pass
    @property
    def CONV1_SIZE(self):
        return self.__CONV1_SIZE
    pass
    @property
    def CONV2_DEEP(self):
        return self.__CONV2_DEEP
    pass
    @property
    def CONV2_SIZE(self):
        return self.__CONV2_SIZE
    pass
    @property
    def CONV3_DEEP(self):
        return self.__CONV3_DEEP
    pass
    @property
    def CONV3_SIZE(self):
        return self.__CONV3_SIZE
    pass
    @property
    def CONV4_DEEP(self):
        return self.__CONV4_DEEP
    pass
    @property
    def CONV4_SIZE(self):
        return self.__CONV4_SIZE
    pass
    @property
    def FC1_SIZE(self):
        return self.__FC1_SIZE
    pass
    @property
    def FC2_SIZE(self):
        return self.__FC2_SIZE
    pass
    ##########################################
    # 模型训练参数
    ##########################################
    @property
    def TRAINING_STEPS(self):
        return self.__TRAINING_STEPS
    pass
    @property
    def BATCH_SIZE(self):
        return self.__BATCH_SIZE
    pass
    @property
    def LEARNING_RATE(self):
        return self.__LEARNING_RATE
    pass
    @property
    def LEARNING_RATE_DECAY(self):
        return self.__LEARNING_RATE_DECAY
    pass
    @property
    def REGULARAZTION_RATE(self):
        return self.__REGULARAZTION_RATE
    pass
    @property
    def MOVING_AVERAGE_DECAY(self):
        return self.__MOVING_AVERAGE_DECAY
    pass
    ##########################################
    # 模型最近checkpoint
    ##########################################
    @property
    def MODEL_SAVE_PATH(self):
        return self.__MODEL_SAVE_PATH
    pass
    @property
    def MODEL_NAME(self):
        return self.__MODEL_NAME
    pass
pass
##########################################
if __name__ == "__main__":
    img = ImgEntry()
    print("IMAGE_PATH=" + img.IMAGE_PATH)
    print("MODEL_SAVE_PATH=" + img.MODEL_SAVE_PATH)
    print("NUM_LABELS=" + img.NUM_LABELS)
    print("MODEL_NAME=" + img.MODEL_NAME)
pass