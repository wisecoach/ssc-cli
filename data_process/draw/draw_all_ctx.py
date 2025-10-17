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


def draw_all_ctx():
    scale = 1.25
    # 读取数据
    data = pd.read_csv('all_ctx.csv')

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()
    x = data['ss']
    width = 0.15  # 柱状图的宽度

    ax1.bar(x - width, data['hmy_throughput'], width, label='HMY')
    ax1.bar(x, data['ssc_throughput'], width, label='CGM-PBFT')
    ax1.bar(x + width, data['ecce_throughput'], width, label='CGM')
    ax1.set_xticks(x)
    ax1.set_xlabel('链式调用次数', fontsize=label_size * scale)
    ax1.set_ylabel('吞吐量', fontsize=label_size * scale)
    ax1.tick_params(axis='both', which='major', labelsize=tick_size * scale, direction='in')
    ax1.set_ylim(0, 120)

    ax2.plot(x, data['hmy_latency'], marker='s', linestyle='--', label='HMY', markersize=marker_size*scale, linewidth=2)
    ax2.plot(x, data['ssc_latency'], marker='s', linestyle='--', label='CGM-PBFT', markersize=marker_size*scale, linewidth=2)
    ax2.plot(x, data['ecce_latency'], marker='s', linestyle='--', label='CGM', markersize=marker_size*scale, linewidth=2)
    ax2.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    ax2.set_ylabel('延迟/s', fontsize=label_size*scale)
    ax2.set_ylim(0, 20)

    ax1.legend(loc='upper left', fontsize=legend_size*scale)

    # 保存图片
    plt.savefig('images/all_ctx.png')
    plt.show()


draw_all_ctx()
