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


def draw_conflict_tx():
    scale = 1.25
    # 读取数据
    data = pd.read_csv('conflict_tx.csv')

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()
    x = data['rate']
    width = 0.15 / 5 # 柱状图的宽度

    ax1.bar(x - width / 2, data['ssc_throughput'], width, label='ssc_throughput')
    ax1.bar(x + width / 2, data['hmy_throughput'], width, label='hmy_throughput')
    ax1.set_xlabel('跨片计数合约调用比例', fontsize=label_size*scale)
    ax1.set_ylabel('吞吐量', fontsize=label_size*scale)
    ax1.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    ax1.set_ylim(0, 100)

    ax2.plot(x, data['ssc_success'], marker='s', linestyle='--', label='ssc_success', markersize=marker_size*scale, linewidth=2, color='green')
    ax2.plot(x, data['hmy_success'], marker='s', linestyle='--', label='hmy_success', markersize=marker_size*scale, linewidth=2, color='red')
    ax2.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    ax2.set_ylabel('成功率/%', fontsize=label_size*scale)
    ax2.set_ylim(0, 100)

    ax1.legend(loc='upper center', fontsize=legend_size*scale)
    ax2.legend(loc='upper right', fontsize=legend_size*scale)

    # 保存图片
    plt.savefig('images/conflict_tx.png')
    plt.show()


draw_conflict_tx()
