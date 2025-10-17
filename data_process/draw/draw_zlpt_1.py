import matplotlib.pyplot as plt
import matplotlib

matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
matplotlib.use('TkAgg')  # 或者 'Agg'


# 数据准备
labels = ['传统区块链', '分片区块链']
sizes = [100, 31]
colors = ['#DAE8FC','#D5E8D4']
explode = (0, 0)  # 仅仅将第一个片（苹果）分裂出来

# 绘制饼状图
plt.pie(sizes, explode=explode, colors=colors, autopct=lambda p: f'{p * sum(sizes) / 100:.0f}%', labels=labels, shadow=True, startangle=0, textprops={'fontsize': 16})

# 确保饼状图是圆形的
plt.axis('equal')

# 显示图表
plt.savefig('images/zlpt_1.png')
plt.show()
