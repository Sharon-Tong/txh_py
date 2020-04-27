#!/usr/bin/env python3
# -*- coding: utf-8 -*-

' a test module '
__author__ = 'Michael Liao'
import sys
import numpy as np
import operator as op
from skimage import io,transform
from com.ctitc.bigdata.common.log.mylog import MyLog


class MyTest():
    age = 82
    def __init__(self,name, score):
        self.__name = name
        self.scare = score
    pass
    def test(self):
        args = sys.argv
        if len(args)==1:
            print(args[0])
            print('Hello, world!')
        elif len(args)==2:
            print('Hello, %s!' % args[1])
        else:
            print('Too many arguments!')
    # def __call__(self, *args, **kwargs):
    #     print(args)
    # pass
pass
if __name__=='__main__':
    fld1 = [[1,2,3,4,5],[5,4,3,2,1]]
    fld1 = np.asarray(fld1)
    x = fld1.copy()
    print(x)
    print(fld1)
    x[1,1] = 2
    print(x)
    print(fld1)

    # box = ['6901028059077', '6901028180849', '6901028059077', '6901028100427', '6901028035613', '6901028225472', '6901028030236', '6901028936231', '6901028131988', '6901028006958', '6901028035644', '6901028124225']
    # pro = [0.9764777421951294, 0.9872114062309265, 0.9765338897705078, 0.9612542390823364, 0.9996405839920044, 0.9721901416778564, 0.9695079326629639, 0.9943337440490723, 0.9954090714454651, 0.895749032497406, 0.9749721884727478, 0.9986690282821655]
    # K=3
    # pro = np.asarray(pro,dtype=np.float64)
    # index = np.argpartition(pro, -K)[-K:]
    #
    # pro = np.asarray(pro)
    # box = np.asarray(box)
    #
    # pro = pro[index]
    # box = box[index]
    #
    # box_dict = {}
    # for indx, key in enumerate(pro):
    #     box_dict[box[indx]] = key
    # pass
    # print(box_dict)
    #
    # y = sorted(box_dict.items(), key=lambda d: d[1], reverse=True)
    # rs = ''
    # for value in y:
    #     if rs == '':
    #         rs += str('{"barCode":%s,"ratio":%s}' % (value[0], value[1]))
    #     else:
    #         rs += str(',{"barCode":%s,"ratio":%s}' % (value[0], value[1]))
    #     pass
    #
    # pass
    #


    # rs2 = '{"barCode":%s,"ratio":%s},' % (box_code[2], box_pro[2])
    # rs1 = '{"barCode":%s,"ratio":%s},' % (box_code[1], box_pro[1])
    # rs0 = '{"barCode":%s,"ratio":%s}' % (box_code[0], box_pro[0])
    # rs = '[' + rs + ']'
    # print(rs)

    # log = MyLog.getLogger()
    # log.info('asdf')
    # print(__doc__)
    # # demo = MyTest('zhangsan',83)(1,2,3)
    # print(callable(MyTest('zhangsan',83)))
    # # demo.test()
pass
