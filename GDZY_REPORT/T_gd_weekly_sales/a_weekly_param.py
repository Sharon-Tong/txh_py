# -*- coding: utf-8 -*-

'''

 Time : 2020/3/30 20:37
 Author : SharonTong

'''
class DailyParam():
    #每年一改
    db_startyear1 = '2019年上半年'
    db_startyear2 = '2019年下半年'
    db_enddate1 = '2020年上半年'
    db_enddate2 = '2020年下半年'
    db_year1 = '2019年'
    db_year2 = '2020年'

    key_order = ['省份', '地市', '品牌', '子类', '规格', '所属工业', '价类', '焦油含量', '规格周长', '包装支数', \
                 '含税调拨价', '建议零售价', '原批发价', '批发价', db_enddate1 + '签订量', db_enddate2 + '签订量', \
                 db_startyear1 + '签订量', db_startyear2 + '签订量', db_enddate1 + '签订额', db_enddate1 + '原签订额', \
                 db_enddate2 + '签订额', db_enddate2 + '原签订额', db_startyear1 + '签订额', db_startyear1 + '原签订额', \
                 db_startyear2 + '签订额', db_startyear2 + '原签订额', db_year2 + '全年签订量', db_year1 + '签订量', db_year2 + '全年签订量额',
                 db_year2 + '全年原签订额', db_year1 + '全年签订量额', db_year1 + '全年原签订额', db_year1 + '全年工业签订额', db_year2 + '全年工业签订额',
                 db_enddate1 + '合同量', db_enddate2 + '合同量']  # 年份每年一改

    title      = r'全国各地市协议合同量'
    filename   = r'D:/2019-2020年全国各地市协议合同量.xlsx'