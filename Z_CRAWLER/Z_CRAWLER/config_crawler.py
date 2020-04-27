#-*- coding: utf-8 -*-
"""
    业务目录参数
"""


class busParam():

    #存放数据的数据表名称
    tb_name  = "dbread.cuntm_dd"

    #获取时间
    sales_year = '2020'
    sales_date = ['3,1-3,1']

    #城市名称
    city = ['广州', '东莞', '中山', '云浮', '佛山','惠州', '揭阳', '梅州', '汕头', '汕尾', '江门', '河源', '清远', '湛江', '潮州', '珠海', '肇庆', '茂名','阳江', '韶关']
    #需要爬虫的规格名称

    #bar_names = ['双喜(硬01)', '双喜(硬)', '双喜(软)', '双喜(软01)', '双喜(软经典)', '双喜(硬经典)', '双喜(硬世纪经典)', '双喜(硬红玫王)', '双喜(珍藏)',
    #             '双喜(硬逸品)', '双喜(经典工坊)','双喜(喜百年)', '双喜(硬经典1906)', '双喜(盛世)', '双喜(软经典1906)', '双喜(和喜)', '双喜(软蓝红玫王)', '双喜(硬蓝红玫王)', '双喜(硬金五叶神)',
    #             '双喜(五叶神金尊)','双喜(红邮喜)', '双喜(硬红五叶神)', '双喜(硬紫红玫王)', '双喜(金01)', '双喜(国喜细支)', '双喜(软红五叶神)', '双喜(大国喜)', '双喜(百年经典)',
    #            '双喜(传奇)','双喜(硬珍藏)', '双喜(金国喜)', '双喜(龙)', '双喜(花悦)', '双喜(春天1979)', '芙蓉王(硬)', '中华(硬)', '中华(软)', '利群(新版)', '贵烟(跨越)']
    bar_names = ['芙蓉王(硬)', '中华(硬)', '中华(软)', '利群(新版)', '贵烟(跨越)']
	#访问链接
    #主链接 & headers
    url = 'https://data.gd.tobacco.com.cn:8443/gkpt/third/industryCommerce/dataAnalysis.jsp'
    headers = {
        'Accept-Ranges': 'bytes',
        'Content-Type': 'application/javascript;charset=utf-8',
        'ETag': 'W/"84345-1558328670000"',
        'Server': 'Apache-Coyote/1.1',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Safari/604.1.38',
        'Referer':'https://data.gd.tobacco.com.cn:8443/gkpt/view.jspx?cubeId=74'
    }
    url2 = 'https://data.gd.tobacco.com.cn:8443/gkpt/server/cube/MODEL_AN_SD_DD_001_CUBES_GY/aggregate?drilldown=date_sale%40ymd%3Aday%7Cco_num%40default%3Aco_num%7Ccom_text_new%40default%3Acom_text_new%7Corgan_name%40default%3Aorgan_name%7Cregie_name%40default%3Aregie_name%7Ccust_code%40default%3Acust_code%7Ccust_name%40default%3Acust_name%7Ccust_type%40default%3Acust_type%7Cstatus_text%40default%3Astatus_text%7Citem_code%40default%3Aitem_code%7Citem_name%40default%3Aitem_name&'

