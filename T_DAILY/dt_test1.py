import pandas
import numpy as np
from pyecharts.charts import Map

from pyecharts.charts import Geo
from pyecharts.globals import ChartType,SymbolType
import pandas   as  pd
from pyecharts.charts import Map
from pyecharts import options as opts
from pyecharts.charts import Timeline

#data_gd = pd.read_excel(r'C:\Users\Tangt\Desktop\可视化\30-42广东双喜(硬1906)avg_price.xlsx',sheet_name='2', index_col='time',engine='xlrd')
data_gd = pd.read_excel(r'C:\Users\Tangt\Desktop\可视化\30-42广东双喜(硬1906)avg_price.xlsx',sheet_name='Sheet1',engine='xlrd')
data=pd.pivot_table(data_gd,index="WEEK_ID",columns='city_name',values='AVG_PRICE_T',aggfunc=sum)
attr = data.columns.tolist()

#缺失值：均值填充
data.fillna(data.mean(), inplace = True)

#取出城市列表
attr = data.columns.tolist()
#统计数据条数
n = len(data.index)

#定义每日地图绘制函数

def map_visualmap(sequence, date) -> Map:
    c = (
        Map()
        .add(date, sequence, maptype="广东")
        .set_global_opts(
            title_opts=opts.TitleOpts(title="广东省双喜(硬经典1906)价格动态地图"),
            visualmap_opts=opts.VisualMapOpts(max_=172,min_=155,
            range_color=["#FFFFFF","#FFCC00","#CC0000"]# 这里修改颜色，低、中、高
            ),
        )
    )
    return c
pass
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
                         title_opts=opts.TitleOpts(title="广东省双喜(硬经典1906)价格动态地图"),
                         visualmap_opts=opts.VisualMapOpts(max_=172, min_=155,
                         range_color=["#FFFFFF", "#FFCC00", "#CC0000"],  # 这里修改颜色，低、中、高
                        is_show=True
                          ))
    )
    return c
pass
def map_visualmap_piece(sequence, date) -> Map:
    c = (
        Map()
        .add(date, sequence, maptype="广东")
        .set_global_opts(
            title_opts=opts.TitleOpts(title="广东省双喜(硬经典1906)价格动态地图"),
            visualmap_opts=opts.VisualMapOpts(is_piecewise=True, pieces=[
                {"min": 168.51, "max": 172, "color": "#FF5151"},
                {"min": 165.5, "max": 168.5, "color": "#FF9797"},
                {"min": 155, "max": 165.4, "color": "#fdebcf"}
            ])
        )
    )
    return c
pass


#创建时间轴对象
timeline_map_visualmap = Timeline()
timeline_geo_lines = Timeline()
timeline_map_visualmap_piece = Timeline()

for i in range(n):
    #取每日数据
    row = data.iloc[i,].tolist()
    #将数据转换为二元的列表
    sequence_temp = list(zip(attr,row))
    #对日期格式化以便显示
    time=str(data.index[i])
    #time = format(data.index[i], "%Y-%w")
    #创建地图
    map_temp = map_visualmap(sequence_temp,time)
    #将地图加入时间轴对象
    timeline.add(map_temp,time).add_schema(play_interval=360)
# 地图创建完成后，通过render()方法可以将地图渲染为html
timeline.render(r'C:\Users\Tangt\Desktop\可视化\广东省双喜动态地图.html')





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
                         title_opts=opts.TitleOpts(title="广东省双喜(硬经典1906)价格动态地图"),
                         visualmap_opts=opts.VisualMapOpts(max_=172, min_=155,
                         range_color=["#FFFFFF", "#FFCC00", "#CC0000"],  # 这里修改颜色，低、中、高
                        is_show=True
                          ))
    )
    return c



#创建时间轴对象
timeline = Timeline()

for i in range(n):
    #取每日数据
    row = data.iloc[i,].tolist()
    #将数据转换为二元的列表
    sequence_temp = list(zip(attr,row))
    #对日期格式化以便显示
    time=str(data.index[i])
    #time = format(data.index[i], "%Y-%w")
    #创建地图w
    #以潮州市为例
    change_list_lr = []
    change_list_lc=[]
    for j in range(len(attr)):
        if data.iloc[i, j] - data.iloc[i, 0] >= 2:
            change_list_lc.append((attr[0], attr[j]))
        if data.iloc[i, j] - data.iloc[i, 0] <= -2:
            chane_list_lr.append((attr[j], attr[0]))
    map_temp = GDLXMap().map_visualmap(sequence_temp,time)
    map_temp_1=GDLXMap().geo_lines(sequence_temp,'流出', '流入',change_list_lc,change_list_lr)
    map_temp_2=GDLXMap().map_visualmap_piece(sequence_temp,time)
    timeline_map_visualmap.add(map_temp_1, time).add_schema(play_interval=100)
    timeline_geo_lines.add(map_temp_1, time).add_schema(play_interval=100)
    timeline_map_visualmap_piece.add(map_temp_1, time).add_schema(play_interval=100)

timeline.render(r'C:\Users\Tangt\Desktop\可视化\广东省双喜(硬经典1906)价格流向地图.html')
timeline.render(r'C:\Users\Tangt\Desktop\可视化\广东省双喜(硬经典1906)价格流向地图.html')
timeline.render(r'C:\Users\Tangt\Desktop\可视化\广东省双喜(硬经典1906)价格流向地图.html')

    map_temp_1 = geo_lines(sequence_temp,'流出', '流入',change_list_lc,change_list_lr)

    #map_temp_1 = geo_lines(sequence_temp,time,change_list)
    #将地图加入时间轴对象
    timeline.add(map_temp_1,time).add_schema(play_interval=100)
    #timeline.add(is_lable_show = True)
# 地图创建完成后，通过render()方法可以将地图渲染为html
timeline.render(r'C:\Users\Tangt\Desktop\可视化\广东省双喜(硬经典1906)价格流向地图.html')


def map_visualmap_piece(sequence, date) -> Map:
    c = (
        Map()
        .add(date, sequence, maptype="广东")
        .set_global_opts(
            title_opts=opts.TitleOpts(title="广东省双喜(硬经典1906)价格动态地图"),
            visualmap_opts=opts.VisualMapOpts(is_piecewise=True, pieces=[
                {"min": 168.51, "max": 172, "color": "#FF5151"},
                {"min": 165.5, "max": 168.5, "color": "#FF9797"},
                {"min": 155, "max": 165.4, "color": "#fdebcf"}
            ])
        )
    )

    return c

#创建时间轴对象
timeline = Timeline()

for i in range(n):
    #取每日数据
    row = data.iloc[i,].tolist()
    #将数据转换为二元的列表
    sequence_temp = list(zip(attr,row))
    #对日期格式化以便显示
    time=str(data.index[i])
    #time = format(data.index[i], "%Y-%w")
    #创建地图w
    map_temp = map_visualmap_piece(sequence_temp,time)
    #将地图加入时间轴对象
    timeline.add(map_temp,time).add_schema(play_interval=360)
# 地图创建完成后，通过render()方法可以将地图渲染为html
timeline.render(r'C:\Users\Tangt\Desktop\可视化\广东省双喜分段动态地图.html')


# visualmap_opts=opts.VisualMapOpts(is_piecewise=True, pieces=[
#             {"min": 1000, "color": "#70161d"},
#             {"min": 500, "max": 1000, "color": "#cb2a2f"},
#             {"min": 100, "max": 500, "color": "#e55a4e"},
#             {"min": 10, "max": 100, "color": "#f59e83"},
#             {"min": 1, "max": 10, "color": "#fdebcf"}
#         ]
