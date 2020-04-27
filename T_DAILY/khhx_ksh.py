import pandas as pd
from pyecharts.charts import Geo
from pyecharts import options as opts
data_gd = pd.read_excel(r'C:\Users\Tangt\Desktop\广州零售户分类结果及属性特征.xlsx',sheet_name='Sheet1')
#print(data_gd.经度)
#print(data_gd.columns.tolist())
#print(data_gd.loc[1,['CITY_NAME']])
geo_cities_coords=dict([(city,[long,lan]) for city,long,lan in zip(data_gd.CITY_NAME,data_gd.经度,data_gd.纬度)])
# #{data_gd.loc[i,['CITY_NAME']]:[data_gd.loc[i,['经度']],data_gd.loc[i,['纬度']]] for i in range(len(data_gd))}
attr = list(data_gd.CITY_NAME)
value=[]
for i in range(len(data_gd)):
    value.append('R_KHTYBM:'+str(data_gd.R_KHTYBM[i])+'R_NAME:'+str(data_gd.R_NAME[i])+'价值类型:'+data_gd.类型[i])

geo = Geo("广州价值客户的客户画像可视化")

geo.add("", attr,value, visual_text_color="#fff",
        symbol_size=15, is_visualmap=True,geo_cities_coords=geo_cities_coords)

geo.render('E:\\maptest2.html')
