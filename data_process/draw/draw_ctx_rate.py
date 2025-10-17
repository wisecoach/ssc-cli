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


def draw_ctx_rate():
    scale = 1.25
    # 读取数据
    data = pd.read_csv('ctx_rate.csv')

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()

    ax1.plot(data['rate'], data['ssc_throughput'], marker='o', label='ssc', markersize=marker_size*scale, linewidth=2)
    ax1.plot(data['rate'], data['hmy_throughput'], marker='s', label='hmy', markersize=marker_size*scale, linewidth=2)
    ax1.set_xlabel('跨分片交易占比', fontsize=label_size*scale)
    ax1.set_ylabel('吞吐量', fontsize=label_size*scale)
    ax1.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    ax1.set_ylim(0, 120)

    ax2.plot(data['rate'], data['ssc_latency'], marker='s', linestyle='--', label='ssc', markersize=marker_size*scale, linewidth=2)
    ax2.plot(data['rate'], data['hmy_latency'], marker='s', linestyle='--', label='hmy', markersize=marker_size*scale, linewidth=2)
    ax2.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    ax2.set_ylabel('延迟/s', fontsize=label_size*scale)
    ax2.set_ylim(0, 12)

    ax1.legend(loc='upper left', fontsize=legend_size*scale)

    # 保存图片
    plt.savefig('images/ctx_rate.png')
    plt.show()


draw_ctx_rate()
