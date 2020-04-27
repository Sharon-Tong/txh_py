
from pyecharts.globals import ChartType
import pandas
from pyecharts.charts import Map3D
from pyecharts.charts import Map
from pyecharts import options as opts
from pyecharts.charts import Timeline

data = pandas.read_excel('E:/可视化/xgyq.xlsx',sheet_name='5', index_col='time',engine='xlrd')
#取出城市列表
attr = data.columns.tolist()
#统计数据条数
n = len(data.index)

JsCode( "function(data){return data.name + " " + data.value[2];}")
# #定义每日地图绘制函数
# def map_visualmap(sequence, date) -> Map:
#     c = (
#         Map()
#         .add(date, sequence,maptype="广东")
#         .set_global_opts(
#             title_opts=opts.TitleOpts(title="广东动态地图"),
#             visualmap_opts=opts.VisualMapOpts(max_=100,
#             range_color=["#FFFFFF","#FFCC00","#CC0000"]# 这里修改颜色，低、中、高
#             ),
#         )
#     )
#     return c
#
# #创建时间轴对象
# timeline = Timeline()
#
# for i in range(n):
#     #取每日数据
#     row = data.iloc[i,].tolist()
#     #将数据转换为二元的列表
#     sequence_temp = list(zip(attr,row))
#     #对日期格式化以便显示
#     time = format(data.index[i], "%Y-%m-%d")
#     #创建地图
#     map_temp = map_visualmap(sequence_temp,time)
#     #将地图加入时间轴对象
#     timeline.add(map_temp,time).add_schema(play_interval=360)
# # 地图创建完成后，通过render()方法可以将地图渲染为html
# timeline.render(r'C:\Users\Tangt\Desktop\可视化\广东动态地图test.html')