# -*- coding: utf-8 -*-

'''

 Time : 2020/3/30 20:37
 Author : SharonTong

'''
class DailyParam():
    #本月的前一个月,每次只改日期
    #双喜全国各地市五率情况
    last_month     = '202001'  # 每月修改一次（5号即可出表），时间为上一月份
    # 全国各区县双喜销售情况表
    start_month_sale = '202001'  # 每年改一次
    end_month_sale = '202001'  # 每月改一次，时间为上一个月


    #双喜全国各地市五率情况
    key_order_prov = ['月份', '省份', '规格码', '规格', '价类', '销量', '覆盖率', '上柜率', '上柜户数', '户均销量', '地市覆盖率', '总户数', \
                      '地市总个数', '区县总个数', '重购户']
    key_order_prov_t = last_month[0:4] + '年'+last_month[4:6]+'月双喜全国各省五率情况'
    key_order_dept_t = last_month[0:4] + '年'+last_month[4:6]+'月双喜全国各地市五率情况'

    key_order_dept = ['月份', '省份', '地市', '规格码', '规格', '价类', '销量', '覆盖率', '上柜率', '上柜户数', '户均销量', '总户数', '区县总个数', '重购户']

    title_dept     = '地市'
    title_prov     = '省份'

    filename       = r'D:/' + last_month[0:4] + '年'+last_month[4:6]+'月双喜各省各地市五率情况.xlsx'

    # 全国各区县双喜销售情况表

    key_order_sale = ['省份', '地市', '区县', '销量','销量','销量','销售额','销售额','销售额']
    key_oder_t     = ['省份', '地市', '区县','双喜销量', '双喜一类销量', '双喜二类销量', '双喜销售额', '双喜一类销售额', '双喜二类销售额']
    key_order_lj   =end_month_sale[0:4] + '年'+start_month_sale[4:6]+'-'+end_month_sale[4:6]+'月全国各省各区县双喜销售情况'
    filename_lj    = r'D:/' + last_month[0:4] + '年'+start_month_sale[4:6]+'-'+end_month_sale[4:6]+'月全国各区县双喜销售情况.xlsx'
    key_order_dy   =end_month_sale[0:4] + '年'+end_month_sale[4:6]+'月全国各省各区县双喜销售情况'
    filename_dy    = r'D:/' + last_month[0:4] + '年'+last_month[4:6]+'月全国各区县双喜销售情况.xlsx'

