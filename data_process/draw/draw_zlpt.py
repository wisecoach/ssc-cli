import matplotlib.pyplot as plt
import matplotlib

matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
matplotlib.use('TkAgg')  # 或者 'Agg'


# 数据准备
labels = ['分片0', '分片1', '分片2', '分片3', '跨分片']
sizes = [15, 21, 31, 25, 8]
colors = ['#DAE8FC','#D5E8D4','#D0CEE2','#B0E3E6', '#F8CECC']
explode = (0, 0, 0, 0, 0)  # 仅仅将第一个片（苹果）分裂出来

# 绘制饼状图
plt.pie(sizes, explode=explode, colors=colors, labels=labels, autopct='%1.0f%%', shadow=True, startangle=140, textprops={'fontsize': 16})

# 确保饼状图是圆形的
plt.axis('equal')

# 显示图表
plt.savefig('images/zlpt.png')
plt.show()
