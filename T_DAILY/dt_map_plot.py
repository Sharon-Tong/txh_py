import pandas as pd
#import phantomjs
from pyecharts.charts import Geo,Map,Timeline
from pyecharts.globals import ChartType,SymbolType
from pyecharts import options as opts
#from ANALYZE.GDZY_REPORT.LOG.MyLog import logger
#from pyecharts_snapshot.main import make_a_snapshot
from pyecharts.render import make_snapshot
#from snapshot_phantomjs import snapshot
#from selenium import webdriver

# chrome_driver=r"C:\Users\Tangt\Desktop\chromedriver_win32\chromedriver.exe"
# driver=webdriver.Chrome(executable_path=chrome_driver)


#外部数据导入
data_gd = pd.read_excel(r'C:\Users\Tangt\Desktop\硬1906广东省流向分析预测结果表.xlsx',sheet_name='Sheet1',engine='xlrd',index_col=0)

#数据透视
#data=pd.pivot_table(data_gd,index="WEEK_ID",columns='city_name',values='AVG_PRICE_T',aggfunc=sum)

#缺失值：均值填充
#data.fillna(data.mean(), inplace = True)
data=data_gd
#取出城市列表
attr = data.columns.tolist()

#统计数据条数
n = len(data.index)



#各地市的价格:type=0:不分段，1：分段，按娜姐的k-means聚类结果分段，高：流入，中：静止，低：流出
def map_visualmap(sequence, date,type) -> Map:
    c = (
        Map()
        .add(date, sequence, maptype="广东")
    )
    if type==0:
        c.set_global_opts(
            title_opts=opts.TitleOpts(title="广东省双喜(硬经典1906)价格动态地图"),
            visualmap_opts=opts.VisualMapOpts(max_=172,min_=155,
            range_color=["#FFFFFF","#FFCC00","#CC0000"]# 这里修改颜色，低、中、高
            ),
        )
    else:
        c.set_global_opts(
            title_opts=opts.TitleOpts(title="广东省双喜(硬经典1906)价格动态地图"),
            visualmap_opts=opts.VisualMapOpts(is_piecewise=True, pieces=[
                {"min": 168.51, "max": 172, "color": "#FF5151"},
                {"min": 165.5, "max": 168.5, "color": "#FF9797"},
                {"min": 155, "max": 165.4, "color": "#fdebcf"}
            ])
        )
    return c
pass


#流向图
def geo_lines(sequence, lr,lc,lr_data,lc_data) -> Geo:
    c = (
        Geo()
        .add_schema(maptype="广东",label_opts=opts.LabelOpts(is_show=True,position='right'),
                    itemstyle_opts=opts.ItemStyleOpts(color='#36648B',border_color='#97FFFF',))
        .add(
            "",
            sequence,
            type_=ChartType.EFFECT_SCATTER,
            is_polyline=True,
            symbol_size=8,is_selected=True
        )
        .add(
            lr,
            lr_data,
            type_=ChartType.LINES,
            effect_opts=opts.EffectOpts(
            symbol=SymbolType.ARROW,
            symbol_size=4,
            color="#33FF33",is_show=True,period=3
            #symbol=2

            ),
            linestyle_opts=opts.LineStyleOpts(curve=0.3,width=0.1,opacity=1,color="#003333"),
            itemstyle_opts=opts.LegendOpts(selected_mode='single',is_show=True,type_='scroll')

        )
        .add(
            lc,
            lc_data,
            type_=ChartType.LINES,
            effect_opts=opts.EffectOpts(
                symbol=SymbolType.ARROW,
                symbol_size=4,
                color="#FFFF33", is_show=True, period=3
                # symbol=2

            ),
            linestyle_opts=opts.LineStyleOpts(curve=0.3, width=0.1, opacity=1, color="red"),
            itemstyle_opts=opts.LegendOpts(selected_mode='single', is_show=True, type_='scroll')

        )
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False),)
        .set_global_opts(
                         title_opts=opts.TitleOpts(title="广东省双喜(硬经典1906)流向动态地图"),
                         visualmap_opts=opts.VisualMapOpts(max_=172, min_=155,
                         range_color=["#FFFFFF", "#FFCC00", "#CC0000"],  # 这里修改颜色，低、中、高
                        is_show=True
                          ))
    )
    return c
pass



#创建时间轴对象
timeline_map_visualmap = Timeline()
timeline_geo_lines = Timeline()
timeline_map_visualmap_piece = Timeline()

def time_part(data,k,timeline_geo_lines,dress):
    #统计数据条数
    n = len(data.index)
    #取出城市列表
    attr = data.columns.tolist()
    for i in range(n):
        # 取每日数据
        row = data.iloc[i,].tolist()
        # 将数据转换为二元的列表
        sequence_temp = list(zip(attr, row))
        # 对日期格式化以便显示
        time = str(data.index[i])
        # time = format(data.index[i], "%Y-%w")
        # 创建地图w
        # 以某个地市为案例，价差大于2为流出，价差小于-2为流入
        change_list_lr = []
        change_list_lc = []
        for j in range(len(attr)):
            if data.iloc[i, j] - data.iloc[i, k] >= 2:
                change_list_lc.append((attr[k], attr[j]))
            if data.iloc[i, j] - data.iloc[i, k] <= -2:
                change_list_lr.append((attr[j], attr[k]))
        map_temp = map_visualmap(sequence_temp, time,type=0)
        map_temp_1 = geo_lines(sequence_temp, '流出', '流入', change_list_lc, change_list_lr)
        map_temp_2 = map_visualmap(sequence_temp, time,type=1)
        # 将地图加入时间轴对象
        #timeline_map_visualmap.add(map_temp, time).add_schema(play_interval=100)
        timeline_geo_lines.add(map_temp_1, time).add_schema(play_interval=100)
        #timeline_map_visualmap_piece.add(map_temp_2, time).add_schema(play_interval=100)
    timeline_geo_lines.render(dress)

time_part(data,0,timeline_geo_lines,r"C:\Users\Tangt\Desktop\可视化\1.广东省双喜(硬经典1906)价格流向地图_xn.html")

# 渲染为html
#timeline_geo_lines.render(r'C:\Users\Tangt\Desktop\可视化\1.广东省双喜(硬经典1906)价格流向地图_xn.html')
#timeline_map_visualmap_piece.render(r'C:\Users\Tangt\Desktop\可视化\2.广东省双喜价格分段动态地图.html')
#timeline_map_visualmap.render(r'C:\Users\Tangt\Desktop\可视化\3.广东省双喜价格动态地图.html')
#make_snapshot(snapshot, timeline_geo_lines.render(),'流向地图.jpg')
