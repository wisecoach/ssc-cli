import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from read_html import resolve_dir

matplotlib.use("TkAgg")
matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决保存图像时负号'-'显示为方块的问题

legend_size = 12
tick_size = 16
label_size = 18
title_size = 16


def draw_fabric():
    resolve_dir("/mnt/E/blockchain/pro-hotstuff-test/caliper-benchmarks/test-prohotstuff", "test-prohotstuff.csv", old_path="/mnt/E/blockchain/pro-hotstuff-test/caliper-benchmarks/test-prohotstuff-old-1")
    resolve_dir("/mnt/E/blockchain/pro-hotstuff-test/caliper-benchmarks/test-bft", "test-bft.csv")
    scale = 1.25
    bft_df = pd.read_csv("test-bft.csv").iloc[:24]
    ph_df = pd.read_csv("test-prohotstuff.csv").iloc[:24]
    fig, ax1 = plt.subplots(nrows=1, ncols=1, figsize=(12, 6))
    ax2 = ax1.twinx()
    ax1.plot(bft_df['tps'], bft_df['Throughput (TPS)'], label='BFT-SMaRt', marker='o', linestyle='-', markersize=8, linewidth=2)
    ax1.plot(ph_df['tps'], ph_df['Throughput (TPS)'], label='repu-PH', marker='o', linestyle='-', markersize=8, linewidth=2)
    ax1.set_xlabel("rate_limit(ops/sec)", fontsize=label_size*scale)
    ax1.set_ylabel('throuput(op/sec)', fontsize=label_size*scale)
    ax1.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    ax2.plot(bft_df['tps'], bft_df['Avg Latency (s)'], label='BFT-SMaRt', marker='o', linestyle='--', markersize=8, linewidth=2)
    ax2.plot(ph_df['tps'], ph_df['Avg Latency (s)'], label='repu-PH', marker='o', linestyle='--', markersize=8, linewidth=2)
    ax2.set_xlabel("rate_limit(ops/sec)", fontsize=label_size*scale)
    ax2.set_ylabel('latency/ms', fontsize=label_size*scale)
    ax2.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    line1, = ax1.plot(bft_df['tps'], bft_df['Throughput (TPS)'], label="throughput", marker='o', linestyle='--', markersize=4, linewidth=2)
    line2, = ax2.plot(bft_df['tps'], bft_df['Avg Latency (s)'], label="latency", marker='o', linestyle='--', markersize=4, linewidth=2)
    line1.set_visible(False)
    line2.set_visible(False)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1[-1:] + lines1[:-1] + lines2[-1:] + lines2[:-1],
               labels1[-1:] + labels1[:-1] + labels2[-1:] + labels2[:-1],
               loc='upper left', ncol=2, fontsize=legend_size*scale, columnspacing=1, handlelength=1.5)
    ax2.set_ylim(0, 6)
    plt.savefig("fabric.png", dpi=300)
    plt.show()


def draw_harmony():
    scale = 1.25
    bft_df = pd.read_csv("harmony-fbft.csv")
    ph_df = pd.read_csv("harmony-prohotstuff.csv")
    fig, ax1 = plt.subplots(nrows=1, ncols=1, figsize=(12, 6))
    ax2 = ax1.twinx()
    ax1.plot(ph_df['tps'], ph_df['Throughput (TPS)'], label='repu-PH', marker='o', linestyle='-', markersize=8, linewidth=2)
    ax1.plot(bft_df['tps'], bft_df['Throughput (TPS)'], label='FBFT', marker='o', linestyle='-', markersize=8, linewidth=2)
    ax1.set_xlabel("rate_limit(ops/sec)", fontsize=label_size*scale)
    ax1.set_ylabel('throuput(op/sec)', fontsize=label_size*scale)
    ax1.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    ax2.plot(ph_df['tps'], ph_df['Avg Latency (s)'], label='repu-PH', marker='o', linestyle='--', markersize=8, linewidth=2)
    ax2.plot(bft_df['tps'], bft_df['Avg Latency (s)'], label='FBFT', marker='o', linestyle='--', markersize=8, linewidth=2)
    ax2.set_xlabel("rate_limit(ops/sec)", fontsize=label_size*scale)
    ax2.set_ylabel('latency/s', fontsize=label_size*scale)
    ax2.tick_params(axis='both', which='major', labelsize=tick_size*scale, direction='in')
    line1, = ax1.plot(bft_df['tps'], bft_df['Throughput (TPS)'], label="throughput", marker='o', linestyle='--', markersize=4, linewidth=2)
    line2, = ax2.plot(bft_df['tps'], bft_df['Avg Latency (s)'], label="latency", marker='o', linestyle='--', markersize=4, linewidth=2)
    line1.set_visible(False)
    line2.set_visible(False)
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1[-1:] + lines1[:-1] + lines2[-1:] + lines2[:-1],
               labels1[-1:] + labels1[:-1] + labels2[-1:] + labels2[:-1],
               loc='upper left', ncol=2, fontsize=legend_size*scale, columnspacing=1, handlelength=1.5)
    ax2.set_ylim(0, 8)
    plt.savefig("harmony.png", dpi=300)
    plt.show()


# draw_fabric()
draw_harmony()
