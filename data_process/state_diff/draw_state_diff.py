from state_diff.process_state_diff import StateDiffProcessor
from state_diff.process_state_diff import (batches, tx_files, itx_files, sd_files)
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.ticker import FuncFormatter
import seaborn as sns
import datashader as ds
import datashader.transfer_functions as tf

matplotlib.rcParams['font.sans-serif'] = ['Simsum']  # 指定默认字体
matplotlib.use('TkAgg')

def get_tx_cnt_data(transpose=False) -> pd.DataFrame:
    data = []
    for name, batch in batches.items():
        ss, us, cs, d = batch
        processor = StateDiffProcessor(20000000, 20010000, tx_files, itx_files, sd_files,
                                       chunk_size=10000, shard_strategy=ss, unlock_strategy=us,
                                       conflict_strategy=cs, max_delay=d)
        processor.calc_conflict_txs()
        tx_cnt_data = processor.to_tx_cnt_data()
        tx_cnt_data['name'] = name
        data.append(tx_cnt_data)
    df = pd.DataFrame(data)
    if transpose:
        df = df.T
    return df


def get_delay_data() -> dict:
    data = {}
    for name, batch in batches.items():
        ss, us, cs, d = batch
        processor = StateDiffProcessor(20000000, 20010000, tx_files, itx_files, sd_files,
                                       chunk_size=10000, shard_strategy=ss, unlock_strategy=us,
                                       conflict_strategy=cs, max_delay=d)
        processor.calc_conflict_txs()
        data[name] = processor.to_delay_data()
    return data


def draw_tx_cnt(df: pd.DataFrame) -> None:
    # 创建图表
    fig, ax = plt.subplots(figsize=(12, 7))

    # 设置柱状图位置和宽度
    x = np.arange(len(df['name']))
    width = 0.22

    # 绘制柱状图
    rects1 = ax.bar(x-width, df['ctx_commit_cnt'], width, label='success', color='green')
    rects2 = ax.bar(x, df['fail'], width, label='fail', color='red')
    rects3 = ax.bar(x+width, df['conflict'], width, label='conflict', color='orange')

    # 设置纵轴格式（以k为单位）
    def thousands_formatter(x, pos):
        return f'{x / 1000:.1f}k'

    ax.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))

    # 添加标签和标题
    ax.set_xlabel('方案类型', fontsize=12, labelpad=10)
    ax.set_ylabel('数量/k', fontsize=12, labelpad=10)
    ax.set_title('不同方案交易数量对比', fontsize=14, fontweight='bold', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(df['name'], fontsize=11)
    ax.legend(loc='upper right', frameon=True, framealpha=0.9)

    def add_labels(rects, offset_factor=1.0):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(f'{height / 1000:.1f}k',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)

    add_labels(rects1)
    add_labels(rects2, offset_factor=0.8)
    add_labels(rects3, offset_factor=0.8)

    # 调整布局
    plt.tight_layout()
    plt.savefig('transaction_comparison.png', dpi=300, bbox_inches='tight')
    plt.show()


def draw_delay_box_plot(dfs: dict) -> None:
    labels = dfs.keys()
    data = [value["delay_times"] for key, value in dfs.items()]

    plt.figure(figsize=(12, 7))

    # 绘制箱线图
    plt.boxplot(data, patch_artist=True, tick_labels=labels, showfliers=False)


    # 添加均值线
    means = [np.mean(d) for d in data]
    # plt.plot(range(1, len(means) + 1), means, 'rs', markersize=8)

    plt.title('Boxplot with Mean Values')
    plt.ylabel('Value')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def draw_delay_error_bar(dfs: dict) -> None:
    labels = dfs.keys()
    data = [value["delays"] for key, value in dfs.items()]

    plt.figure(figsize=(12, 7))

    # 添加均值线
    means = [np.mean(d) for d in data]
    stds = [np.std(d) for d in data]

    plt.bar(range(1, len(means) + 1), means, yerr=stds)
    plt.errorbar(range(1, len(means) + 1), means, stds, fmt='-o', capsize=5)
    # plt.errorbar(range(1, len(means) + 1), means, stds, fmt='-o', capsize=5)

    plt.title('Boxplot with Mean Values')
    plt.ylabel('Value')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def draw_delay_swarm_plot(dfs: dict) -> None:
    labels = dfs.keys()
    data = [value["delays"] for key, value in dfs.items()]

    plt.figure(figsize=(12, 7))

    # 添加均值线
    means = [np.mean(d) for d in data]
    stds = [np.std(d) for d in data]

    plt.bar(range(1, len(means) + 1), means, yerr=stds)
    plt.errorbar(range(1, len(means) + 1), means, stds, fmt='-o', capsize=5)
    # plt.errorbar(range(1, len(means) + 1), means, stds, fmt='-o', capsize=5)

    plt.title('Boxplot with Mean Values')
    plt.ylabel('Value')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def draw_delay_violin(dfs: dict) -> None:
    d = []
    for key, data in dfs.items():
        if key in ['ssc_2:1', 'ssc_2:5']:
            continue
        for delay in data['delays']:
            delay = min(delay, 20)
            d.append({"category": key, "value": delay})
    df = pd.DataFrame(d)

    plt.figure(figsize=(12, 7))

    sns.violinplot(
        x="category",
        y="value",
        data=df,
        cut=2,
        bw_method='scott',
        inner="quartile",  # 内嵌箱线图
    )

    plt.title('Boxplot with Mean Values')
    plt.ylabel('Value')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def draw_delay_plot(dfs: dict) -> None:
    labels = dfs.keys()
    data = [value["delays"] for key, value in dfs.items()]
    x = np.arange(21)
    ys = []
    for dt in data:
        cnt = [0] * 21
        for d in dt:
            if d <= 20:
                cnt[d] += 1
        narray = np.array(cnt) + 1
        ys.append(np.log10(narray))

    fig, ax1 = plt.subplots(figsize=(12, 7))
    ax1.set_xticks(np.arange(0, 21, 1))
    for y, label in zip(ys, labels):
        ax1.plot(x, y, 'o-', label=label)

    plt.title('Boxplot with Mean Values')
    plt.ylabel('Value')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def draw_delay_heatmap(dfs: dict) -> None:
    plt.figure(figsize=(12, 7))
    ys = []
    data = [value["delays"] for key, value in dfs.items()]

    for dt in data:
        cnt = [0] * 22
        for d in dt:
            if d <= 20:
                cnt[d] += 1
            else:
                cnt[21] += 1
        narray = np.array(cnt)
        narray = np.where(narray > 0, narray, 1)
        ys.append(np.log10(narray))
    df_data = []
    for zs, x in zip(ys, dfs.keys()):
        for y, z in enumerate(zs):
            if y > 0:
                df_data.append({"x": x, "y": y, "z": z})
    df = pd.DataFrame(df_data)
    heatmap_data = df.pivot(index="y", columns="x", values="z")
    heatmap_pct = heatmap_data.div(heatmap_data.sum(axis=0), axis=1) * 100
    annot_text = heatmap_pct.map(lambda x: f"{x:.0f}%")  # 自定义标注文本
    sns.heatmap(heatmap_data, annot=annot_text, fmt='s', cmap='viridis')
    plt.xlabel('X')
    plt.ylabel('Y')
    ticks = np.arange(1, 22, 1)
    ticklabels = [f"{tick}" for tick in ticks]
    ticklabels[-1] = "20+"
    plt.yticks(ticks, ticklabels)
    plt.title("Contour Heatmap of Z")
    plt.show()


# delay_data = get_delay_data()
# draw_delay_box_plot(delay_data)
# draw_delay_violin(delay_data)
# draw_delay_plot(delay_data)
# draw_delay_heatmap(delay_data)
# draw_delay_error_bar(delay_data)
tx_cnt_data = get_tx_cnt_data()
draw_tx_cnt(tx_cnt_data)








