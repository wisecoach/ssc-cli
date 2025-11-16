import time

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib

matplotlib.rcParams['font.sans-serif'] = ['Simsum']  # 指定默认字体
matplotlib.use('TkAgg')

def process_harmony() -> dict:
    df = pd.read_csv('../output/simulate_tcc_results.csv')
    df = df[:1000]
    from_time = df['start_time'].min()
    to_time = df['end_time'].max()
    committed_sum = df['commit'].sum()
    filtered_df = df[df['commit'] == True]
    delay = (filtered_df['end_time'] - filtered_df['start_time']).mean()
    return {
        'name': 'harmony',
        'ctx_commit_cnt': committed_sum,
        'tps': committed_sum / (to_time - from_time),
        'delay': delay,
    }

def process_ssc(timeout) -> dict:
    df = pd.read_csv(f'../output/processed_txs_{timeout}.csv')
    from_time = pd.to_datetime(df.dropna(subset='start_time')['start_time'], format='ISO8601').min()
    to_time = pd.to_datetime(df.dropna(subset='end_time')['end_time'], format='ISO8601').max()
    filtered_df = df[df['status'] == 'commit']
    committed_sum = len(filtered_df)
    delay = filtered_df['time'].mean()
    return {
        'name': f'ssc_timeout_{timeout}',
        'ctx_commit_cnt': committed_sum,
        'tps': committed_sum / (to_time - from_time).total_seconds(),
        'delay': delay,
    }

def process_data() -> pd.DataFrame:
    data = []
    data.append(process_harmony())
    data.append(process_ssc(5))
    data.append(process_ssc(10))
    data.append(process_ssc(15))
    df = pd.DataFrame(data)
    return df

def draw_base(df: pd.DataFrame) -> None:
    # 创建图表
    fig, ax1 = plt.subplots(figsize=(12, 7))
    ax2 = ax1.twinx()

    # 设置柱状图位置和宽度
    x = np.arange(len(df['name']))
    width = 0.22

    # 绘制柱状图
    ax1.bar(x, df['tps'], width, label='tps')
    ax2.plot(x, df['delay'], label='delay', color='orange', marker='o')
    ax2.set_ylim(0, df['delay'].max() * 1.2)

    # 添加标签和标题
    ax1.set_xlabel('scheme', fontsize=12, labelpad=10)
    ax1.set_ylabel('tps', fontsize=12, labelpad=10)
    ax2.set_ylabel('delay/s', fontsize=12, labelpad=10)
    ax1.set_xticks(x)
    ax1.set_xticklabels(df['name'], fontsize=11)
    fig.legend(loc='upper right', frameon=True, framealpha=0.9)

    # def add_labels(rects, offset_factor=1.0):
    #     for rect in rects:
    #         height = rect.get_height()
    #         ax.annotate(f'{height / 1000:.1f}k',
    #                     xy=(rect.get_x() + rect.get_width() / 2, height),
    #                     xytext=(0, 3),  # 3 points vertical offset
    #                     textcoords="offset points",
    #                     ha='center', va='bottom', fontsize=9)
    #
    # add_labels(rects1)
    # add_labels(rects2, offset_factor=0.8)
    # add_labels(rects3, offset_factor=0.8)

    # 调整布局
    plt.tight_layout()
    plt.savefig('transaction_comparison.png', dpi=300, bbox_inches='tight')
    plt.show()


draw_base(process_data())