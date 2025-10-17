import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
matplotlib.use('TkAgg')

legend_size = 14
tick_size = 16
label_size = 18
title_size = 16
marker_size = 8


def draw_ss():
    scale = 1.25
    # 读取数据
    data = pd.read_csv('shard_num.csv')

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()
    x = data['shard']
    x_t = np.arange(1, len(x)+1)
    width = 0.15  # 柱状图的宽度

    ax1.bar(x_t - width / 2, data['ssc_throughput'], width, label='ssc_throughput')
    ax1.bar(x_t + width / 2, data['hmy_throughput'], width, label='hmy_throughput')
    ax1.set_xticks([1, 2, 3, 4, 5])
    ax1.set_xticklabels(['1', '2', '3', '4', '5'])
    ax1.set_xlabel('分片数量', fontsize=label_size*scale)
    ax1.set_ylabel('吞吐量', fontsize=label_size*scale)
    ax1.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    ax1.set_ylim(0, 400)

    ax2.plot([1, 2, 3, 4, 5], data['ssc_latency'], marker='s', linestyle='--', label='ssc_latency', markersize=marker_size*scale, linewidth=2, color='green')
    ax2.plot([1, 2, 3, 4, 5], data['hmy_latency'], marker='s', linestyle='--', label='hmy_latency', markersize=marker_size*scale, linewidth=2, color='red')
    ax2.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    ax2.set_ylabel('延迟/s', fontsize=label_size*scale)
    ax2.set_ylim(0, 8)

    ax1.legend(loc='upper left', fontsize=legend_size*scale)
    ax2.legend(loc='upper center', fontsize=legend_size*scale)

    # 保存图片
    plt.savefig('images/shard_num.png')
    plt.show()


draw_ss()
