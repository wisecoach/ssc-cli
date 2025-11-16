import pandas as pd
import pickle
import os
import math
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

HEX_TO_BITS = {
    '0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7,
    '8': 8, '9': 9, 'A': 10, 'B': 11, 'C': 12, 'D': 13, 'E': 14, 'F': 15,
    'a': 10, 'b': 11, 'c': 12, 'd': 13, 'e': 14, 'f': 15
}

def hex_to_binary_classes_head(hex_str, n):
    # 直接计算掩码（避免字符串操作）
    mask = (1 << n) - 1  # 前n位的掩码，例如 n=4 时 mask=0b1111
    bits = 0
    length = 0
    # 遍历16进制字符，逐步构建二进制值
    for c in hex_str[2:]:
        bits = (bits << 4) | HEX_TO_BITS[c]  # 左移4位并合并新字符的4位
        length += 4
        if length >= n:  # 已累积足够的位数
            break
    # 右移多余位数并应用掩码
    shift = max(length - n, 0)
    class_num = (bits >> shift) & mask
    return class_num


def hex_to_binary_classes_tail(hex_str, n):
    mask = (1 << n) - 1  # 末尾n位的掩码，例如 n=4 时 mask=0b1111
    bits = 0
    length = 0
    # 从右向左遍历16进制字符
    for c in reversed(hex_str):
        bits = (HEX_TO_BITS[c] << length) | bits  # 将当前字符的4位添加到左侧
        length += 4
        if length >= n:  # 已累积足够的位数
            break
    # 直接截取末尾n位
    class_num = bits & mask
    return class_num

class CTXProcessor:
    def __init__(self, from_block, to_block, tx_files, itx_files, chunk_size = 1000, shard_strategy="prefix_4"):
        self.shard_strategy = shard_strategy
        self.shard_way, self.shard_bit_num = self.shard_strategy.split("_")
        self.shard_bit_num = int(self.shard_bit_num)
        self.cache_dir = f"block_{from_block}-{to_block}_shard_{shard_strategy}"
        self.from_block = from_block
        self.to_block = to_block
        self.progress = None
        self.matrix = None
        self.progress = None
        self.load_progress()
        self.itx_files = itx_files
        self.tx_files = tx_files
        self.chunk_size = chunk_size
        self.tx_gen = self.block_generator(self.tx_files, self.progress['block_num'])
        self.itx_gen = self.block_generator(self.itx_files, self.progress['block_num'])

    def save_progress(self):
        print(f"save progress to {self.cache_dir}")
        with open(os.path.join(self.cache_dir, 'progress.pkl'), 'wb') as f:
            pickle.dump(self.progress, f)
        with open(os.path.join(self.cache_dir, 'matrix.pkl'), 'wb') as f:
            pickle.dump(self.matrix, f)

    def load_progress(self):
        print(f"load progress from {self.cache_dir}")
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)
        if os.path.exists(os.path.join(self.cache_dir, 'progress.pkl')):
            with open(os.path.join(self.cache_dir, 'progress.pkl'), 'rb') as f:
                self.progress = pickle.load(f)
        else:
            self.progress = {'finish': False, 'from_block': self.from_block, 'to_block': self.to_block, 'block_num': 0, 'cross_call': 0, 'cross_cnt': 0, 'cross_transfer': 0, 'inner_call': 0,
                             'inner_cnt': 0, 'inner_transfer': 0}
        if os.path.exists(os.path.join(self.cache_dir, 'matrix.pkl')):
            with open(os.path.join(self.cache_dir, 'matrix.pkl'), 'rb') as f:
                self.matrix = pickle.load(f)
                self.matrix = defaultdict(int, self.matrix)
        else:
            self.matrix = defaultdict(int)

    def matrix_to_csv(self, min_call=1, max_call=10):
        data = []
        for (row, col), value in self.matrix.items():
            if min_call <= row <= max_call and col <= max_call:
                data.append({"X": row, "Y": col, "Z": math.log10(value)})
        df = pd.DataFrame(data)
        df.to_csv(os.path.join(self.cache_dir, 'matrix.csv'))

    def csv_to_contourf(self):
        df = pd.read_csv(os.path.join(self.cache_dir, 'matrix.csv'))
        df_pivot = df.pivot(index='X', columns='Y', values='Z')
        X = df_pivot.columns.values
        Y = df_pivot.index.values
        Z = df_pivot.values
        plt.figure(figsize=(8, 6))
        contour = plt.contourf(X, Y, Z, levels=100, cmap='viridis')
        plt.colorbar(contour, label='Counts (Z)')
        plt.xlabel('X')
        plt.ylabel('Y')
        plt.title("Contour Heatmap of Z")
        plt.show()

    def csv_to_heatmap(self):
        df = pd.read_csv(os.path.join(self.cache_dir, 'matrix.csv'))
        heatmap_data = df.pivot(index='X', columns='Y', values='Z')
        plt.figure(figsize=(8, 6))
        sns.heatmap(heatmap_data, annot=True, fmt='.1f', cmap='viridis')  # annot=True 显示数值
        plt.title("Heatmap of Counts (Z) by X and Y")
        plt.show()

    def csv_to_heatmap_percent(self):
        df = pd.read_csv(os.path.join(self.cache_dir, 'matrix.csv'))
        heatmap_data = df.pivot(index='X', columns='Y', values='Z')
        heatmap_original = 10 ** heatmap_data
        row_sums = heatmap_original.sum(axis=1)
        total_sum = row_sums.sum()
        heatmap_pct = heatmap_original / total_sum * 100
        annot_text = heatmap_pct.map(lambda x: f"{x:.1f}%")  # 自定义标注文本
        plt.figure(figsize=(8, 6))
        ax = sns.heatmap(heatmap_data, annot=annot_text, fmt='s', cmap='viridis', cbar_kws={'label': 'Log10(Transaction Count)'})  # annot=True 显示数值
        # 获取当前的刻度标签
        x_labels = [t.get_text() for t in ax.get_xticklabels()]
        y_labels = [t.get_text() for t in ax.get_yticklabels()]

        # 替换标签
        x_labels = [f"≥{label}" if label == "10" else label for label in x_labels]
        y_labels = [f"≥{label}" if label == "10" else label for label in y_labels]

        # 设置新的刻度标签
        ax.set_xticklabels(x_labels)
        ax.set_yticklabels(y_labels)
        plt.xlabel('Cross-Shard Contract Calls')
        plt.ylabel('Total Contract Calls')
        plt.savefig(f"../img/Heatmap of Transaction Count by Total and Cross-Shard Contract Calls,pref={self.shard_strategy}.png",
                    dpi=300,
                    bbox_inches='tight',
                    pad_inches=0)
        plt.show()

    def block_generator(self, files, start_block):
        cache = None
        current_block = None

        for csv_path in files:
            # 使用迭代器逐块读取CSV，避免一次性加载
            reader = pd.read_csv(csv_path, chunksize=self.chunk_size)
            for chunk in reader:
                last_block = chunk.iloc[-1]['blockNumber']
                if last_block < start_block:
                    print(f"the last block is {last_block}/{start_block}, skipping")
                    continue
                # 按blockNumber分组，不排序（假设输入已全局有序）
                grouped = chunk.groupby('blockNumber', sort=False)

                for block_num, group in grouped:
                    if current_block is None:
                        # 初始化第一个block
                        current_block = block_num
                        cache = group.copy()
                    elif block_num == current_block:
                        # 合并到当前缓存
                        cache = pd.concat([cache, group], ignore_index=True)
                    else:
                        # 遇到新block，返回当前缓存
                        yield current_block, cache
                        # 更新为新block的数据
                        current_block = block_num
                        cache = group.copy()
        # 返回最后一个block的数据
        if current_block is not None:
            yield current_block, cache

    def get_shard(self, addr):
        if self.shard_way == "prefix":
            return hex_to_binary_classes_head(addr, self.shard_bit_num)
        elif self.shard_way == "suffix":
            return hex_to_binary_classes_tail(addr, self.shard_bit_num)
        else:
            raise ValueError

    def cross_shard(self, from_addr, to_addr):
        # nan表示合约创建，不属于任何分片
        if type(to_addr) is float and math.isnan(to_addr):
            return False
        # 普通int为预编译合约，不属于任何分片
        if type(to_addr) is int:
            return False
        return self.get_shard(from_addr) != self.get_shard(to_addr)

    def average_and_sum_call_chain(self, chain_count):
        total = 0
        cnt = 0
        for k, v in chain_count.items():
            total += k * v
            cnt += v
        return total/cnt, total

    def calc_cross_call_chain(self):
        cur_itxs = None
        cur_block_number = 0
        if self.progress['finish']:
            return
        if self.progress['block_num'] == self.to_block - 1:
            self.progress['finish'] = True
            self.save_progress()
            return

        for block_num, txs_df in self.tx_gen:
            if block_num < self.progress['block_num']:
                if block_num % 100 == 0:
                    print(f"skip: [{block_num}/21000000]")
                continue
            while cur_block_number < block_num:
                cur_block_number, cur_itxs = next(self.itx_gen)
            # 存在内部交易
            if cur_block_number == block_num:
                tx_map = {}
                for _, tx in txs_df.iterrows():
                    tx_map[tx['transactionHash']] = {
                        'transactionHash': tx['transactionHash'],
                        'from': tx['from'],
                        'to': tx['to'],
                        'call_count': 0,
                        'cross_call_count': 0,
                        'cross-shard': False,
                    }
                for _, itx in cur_itxs.iterrows():
                    if itx['transactionHash'] in tx_map:
                        tx_map[itx['transactionHash']]['call_count'] += 1
                        if self.cross_shard(itx['from'], itx['to']):
                            tx_map[itx['transactionHash']]['cross_call_count'] += 1
                            tx_map[itx['transactionHash']]['cross-shard'] = True
                for _, tx in tx_map.items():
                    self.matrix[(tx['call_count'], tx['cross_call_count'])] += 1
                    if tx['call_count'] == 0:
                        if self.cross_shard(tx['from'], tx['to']):
                            self.progress['cross_transfer'] += 1
                            self.progress['cross_cnt'] += 1
                        else:
                            self.progress['inner_transfer'] += 1
                            self.progress['inner_cnt'] += 1
                    else:
                        if tx['cross-shard']:
                            self.progress['cross_cnt'] += 1
                        else:
                            self.progress['inner_cnt'] += 1
                    self.progress['cross_call'] += tx['cross_call_count']
                    self.progress['inner_call'] += tx['call_count'] - tx['cross_call_count']
            else:
                for _, tx in txs_df.iterrows():
                    if self.cross_shard(tx['from'], tx['to']):
                        self.progress['cross_transfer'] += 1
                        self.progress['cross_cnt'] += 1
                    else:
                        self.progress['inner_transfer'] += 1
                        self.progress['inner_cnt'] += 1
            if block_num % 10 == 0:
                print(f"complete: [{block_num}/21000000], cross_cnt: {self.progress['cross_cnt']}, inner_cnt: {self.progress['inner_cnt']}, cross_transfer: {self.progress['cross_transfer']}, inner_transfer: {self.progress['inner_transfer']}"
                      f", cross_call: {self.progress['cross_call']}, inner_call: {self.progress['inner_call']}")
            if block_num % 1000 == 0:
                self.progress['block_num'] = block_num
                self.save_progress()
            if block_num == self.to_block - 1:
                self.progress['finish'] = True
                self.save_progress()


tx_files = ['data/20000000to20249999_BlockTransaction.csv', 'data/20250000to20499999_BlockTransaction.csv', 'data/20500000to20749999_BlockTransaction.csv', 'data/20750000to20999999_BlockTransaction.csv']
itx_files = ['data/20000000to20249999_InternalTransaction.csv', 'data/20250000to20499999_InternalTransaction.csv', 'data/20500000to20749999_InternalTransaction.csv', 'data/20750000to20999999_InternalTransaction.csv']
for shard_strategy in ["prefix_2", "prefix_3"]:
    processor = CTXProcessor(20000000, 21000000, tx_files, itx_files, chunk_size=10000, shard_strategy=shard_strategy)
    processor.calc_cross_call_chain()
    # processor.matrix_to_csv(max_call=10)
    processor.csv_to_heatmap_percent()

