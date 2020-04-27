# -*- coding: utf-8 -*-
'''
Author : Sharon_Tong
Time   : 2020/4/2 0:02
'''

import zipfile
import os
from GDZY_REPORT.LOG.MyLog import logger
import tarfile
from unrar import rarfile
class Unzip():
    # 获得目录下的所有文件
    def get_file_name(self,file_dir,key_word=None):
        self.Filelist = []
        self.AllFilelist = []
        for home, dirs, files in os.walk(file_dir):
            self.home=home
            for file in files:
                if key_word!=None:
                    if key_word in file:
                        self.AllFilelist.append(os.path.join(home, file))
                        self.Filelist.append(file)
                else:
                    self.AllFilelist.append(os.path.join(home, file))
                    self.Filelist.append(file)
            pass
        return self.AllFilelist

    #解压压缩包
    def unkwzip(self,unzipfilename,zip_type):
        zipfilename = os.path.join(self.home,unzipfilename)
        logger.get_logger().info('开始解压...')
        if os.path.exists(zipfilename):
            pass
        else:
            os.makedirs(zipfilename)
        i=0
        #开始循环解压，将压缩文件中的文件解压
        for name in self.Filelist:
            filename=os.path.join(self.home,name)
            logger.get_logger().info('解压的文件是：'+filename)
            if zip_type == 'zip':
                z = zipfile.ZipFile(filename)
                z.extractall(zipfilename)
            if zip_type == 'tar':
                z = tarfile.open(filename)
                z.extractall(zipfilename)
            if zip_type == 'rar':
                z = rarfile.RarFile(filename)
                z.extractall(zipfilename)

            i+=1
        pass
        if zip_type == 'rar':
            pass
        else:
            z.close()
        logger.get_logger().info('解压结束...')
        logger.get_logger().info('共解压%d个压缩文件'% i)
        return zipfilename
    pass
pass



# a = Unzip()
# file = a.get_file_name(r'D:\ctitc','')
# a.unkwzip('test','rar')
