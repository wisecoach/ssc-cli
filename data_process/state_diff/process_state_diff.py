import json

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


def hex_to_binary_classes_head(hex_str:str, n:int) -> int:
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


class StateDiffProcessor:
    NORMAL_UNLOCK_STRATEGY = "normal"
    SSC_UNLOCK_STRATEGY = "ssc"
    DELAY_CONFLICT_STRATEGY = "delay"
    DELAY_TO_FAIL_STRATEGY = "delay-to-fail"
    FAIL_CONFLICT_STRATEGY = "fail"

    def __init__(self, from_block, to_block, tx_files, itx_files, sd_files, chunk_size=1000, shard_strategy="prefix_4",
                 unlock_strategy=NORMAL_UNLOCK_STRATEGY, conflict_strategy=FAIL_CONFLICT_STRATEGY, max_delay=10):
        self.shard_strategy = shard_strategy
        self.shard_way, self.shard_bit_num = self.shard_strategy.split("_")
        self.shard_bit_num = int(self.shard_bit_num)
        self.cache_dir = f"block_{from_block}-{to_block}_shard_{shard_strategy}_unlock_{unlock_strategy}_conflict_{conflict_strategy}_delay_{max_delay}"
        self.from_block = from_block
        self.to_block = to_block
        self.progress = {}
        self.delays = []
        self.delay_times = []
        self.matrix = None
        self.conflict_addr2cnt = defaultdict(int)
        self.states2num2commit = None
        self.num2unlock_state = defaultdict(dict)
        self.uncommitted_txs = {}
        self.num2commit_tx_hash = defaultdict(dict)
        self.load_progress()
        self.itx_files = itx_files
        self.tx_files = tx_files
        self.sd_files = sd_files
        self.chunk_size = chunk_size
        self.unlock_strategy = unlock_strategy
        self.conflict_strategy = conflict_strategy
        self.max_delay = max_delay
        self.tx_gen = self.block_generator(self.tx_files, self.progress['block_num'])
        self.itx_gen = self.block_generator(self.itx_files, self.progress['block_num'])
        self.sd_gen = self.sd_block_generator(self.sd_files, self.progress['block_num'])

    def save_progress(self):
        print(f"save progress to {self.cache_dir}")
        with open(os.path.join(self.cache_dir, 'progress.pkl'), 'wb') as f:
            pickle.dump(self.progress, f)
        with open(os.path.join(self.cache_dir, 'progress.json'), 'w') as f:
            json.dump(self.progress, f)
        with open(os.path.join(self.cache_dir, 'delays.pkl'), 'wb') as f:
            pickle.dump(self.delays, f)
        with open(os.path.join(self.cache_dir, 'delays.json'), 'w') as f:
            json.dump(self.delays, f)
        with open(os.path.join(self.cache_dir, 'delay_times.pkl'), 'wb') as f:
            pickle.dump(self.delay_times, f)
        # with open(os.path.join(self.cache_dir, 'delay_times.json'), 'w') as f:
        #     json.dump(self.delay_times, f)
        with open(os.path.join(self.cache_dir, 'matrix.pkl'), 'wb') as f:
            pickle.dump(self.matrix, f)
        with open(os.path.join(self.cache_dir, 'matrix.json'), 'w') as f:
            json.dump(self.matrix, f)
        with open(os.path.join(self.cache_dir, 'states2num2commit.pkl'), 'wb') as f:
            pickle.dump(self.states2num2commit, f)
        with open(os.path.join(self.cache_dir, 'states2num2commit.json'), 'w') as f:
            json.dump(self.states2num2commit, f)
        with open(os.path.join(self.cache_dir, 'conflict_addr2cnt.pkl'), 'wb') as f:
            pickle.dump(self.conflict_addr2cnt, f)
        with open(os.path.join(self.cache_dir, 'conflict_addr2cnt.json'), 'w') as f:
            json.dump(self.conflict_addr2cnt, f)
        with open(os.path.join(self.cache_dir, 'uncommitted_txs.pkl'), 'wb') as f:
            pickle.dump(self.uncommitted_txs, f)
        with open(os.path.join(self.cache_dir, 'uncommitted_txs.json'), 'w') as f:
            json.dump(self.uncommitted_txs, f)
        with open(os.path.join(self.cache_dir, 'num2commit_tx_hash.pkl'), 'wb') as f:
            pickle.dump(self.num2commit_tx_hash, f)
        with open(os.path.join(self.cache_dir, 'num2commit_tx_hash.json'), 'w') as f:
            json.dump(self.num2commit_tx_hash, f)

    def load_progress(self):
        print(f"load progress from {self.cache_dir}")
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)
        if os.path.exists(os.path.join(self.cache_dir, 'progress.pkl')):
            with open(os.path.join(self.cache_dir, 'progress.pkl'), 'rb') as f:
                self.progress = pickle.load(f)
        else:
            self.progress = {'finish': False, 'from_block': self.from_block, 'to_block': self.to_block, 'block_num': 0,
                             'tx_cnt': 0, 'ctx_cnt': 0, 'ctx_commit_cnt': 0, 'delay_cnt': 0, 'conflict': 0, 'fail': 0}
        if os.path.exists(os.path.join(self.cache_dir, 'delays.pkl')):
            with open(os.path.join(self.cache_dir, 'delays.pkl'), 'rb') as f:
                self.delays = pickle.load(f)
        else:
            self.delays = []
        if os.path.exists(os.path.join(self.cache_dir, 'delay_times.pkl')):
            with open(os.path.join(self.cache_dir, 'delay_times.pkl'), 'rb') as f:
                self.delay_times = pickle.load(f)
        else:
            self.delay_times = []
        if os.path.exists(os.path.join(self.cache_dir, 'matrix.pkl')):
            with open(os.path.join(self.cache_dir, 'matrix.pkl'), 'rb') as f:
                self.matrix = pickle.load(f)
                self.matrix = defaultdict(dict, self.matrix)
        else:
            self.matrix = defaultdict(dict)
        if os.path.exists(os.path.join(self.cache_dir, 'states2num2commit.pkl')):
            with open(os.path.join(self.cache_dir, 'states2num2commit.pkl'), 'rb') as f:
                self.states2num2commit = pickle.load(f)
                self.states2num2commit = defaultdict(int, self.states2num2commit)
                for state, num in self.num2unlock_state.items():
                    self.num2unlock_state[num][state] = True
        else:
            self.states2num2commit = defaultdict(int)
        if os.path.exists(os.path.join(self.cache_dir, 'conflict_addr2cnt.pkl')):
            with open(os.path.join(self.cache_dir, 'conflict_addr2cnt.pkl'), 'rb') as f:
                self.conflict_addr2cnt = pickle.load(f)
                self.conflict_addr2cnt = defaultdict(int, self.conflict_addr2cnt)
        else:
            self.conflict_addr2cnt = defaultdict(int)
        if os.path.exists(os.path.join(self.cache_dir, 'uncommitted_txs.pkl')):
            with open(os.path.join(self.cache_dir, 'uncommitted_txs.pkl'), 'rb') as f:
                self.uncommitted_txs = pickle.load(f)
        if os.path.exists(os.path.join(self.cache_dir, 'num2commit_tx_hash.pkl')):
            with open(os.path.join(self.cache_dir, 'num2commit_tx_hash.pkl'), 'rb') as f:
                self.num2commit_tx_hash = pickle.load(f)
                self.num2commit_tx_hash = defaultdict(dict, self.num2commit_tx_hash)
        else:
            self.num2commit_tx_hash = defaultdict(dict)

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
        heatmap_pct = heatmap_original.div(heatmap_original.sum(axis=1), axis=0) * 100
        annot_text = heatmap_pct.map(lambda x: f"{x:.0f}%")  # 自定义标注文本
        plt.figure(figsize=(8, 6))
        sns.heatmap(heatmap_data, annot=annot_text, fmt='s', cmap='viridis')  # annot=True 显示数值
        plt.title("Heatmap of Counts (Z) by X and Y")
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

    def sd_block_generator(self, files, start_block):
        cache = None
        current_block = None

        for csv_path in files:
            # 使用迭代器逐块读取CSV，避免一次性加载
            reader = pd.read_csv(csv_path, chunksize=self.chunk_size)
            for chunk in reader:
                last_block = chunk.iloc[-1]['block_number']
                if last_block < start_block:
                    print(f"the last block is {last_block}/{start_block}, skipping")
                    continue
                # 按block_number分组，不排序（假设输入已全局有序）
                grouped = chunk.groupby('block_number', sort=False)

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
        if type(addr) == float:
            print(f"addr is float: {addr}")
            return 0
        if self.shard_way == "prefix":
            return hex_to_binary_classes_head(addr, self.shard_bit_num)
        elif self.shard_way == "suffix":
            return hex_to_binary_classes_tail(addr, self.shard_bit_num)
        else:
            raise ValueError

    @staticmethod
    def ignore_addr(addr) -> bool:
        if type(addr) == float:
            return True
        return False

    def cross_shard(self, from_addr, to_addr):
        # nan表示合约创建，不属于任何分片
        if type(to_addr) is float and math.isnan(to_addr):
            return False
        # 普通int为预编译合约，不属于任何分片
        if type(to_addr) is int:
            return False
        return self.get_shard(from_addr) != self.get_shard(to_addr)

    @staticmethod
    def average_and_sum_call_chain(chain_count):
        total = 0
        cnt = 0
        for k, v in chain_count.items():
            total += k * v
            cnt += v
        return total / cnt, total

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
                print(
                    f"complete: [{block_num}/{self.to_block}], cross_cnt: {self.progress['cross_cnt']}, inner_cnt: {self.progress['inner_cnt']}, cross_transfer: {self.progress['cross_transfer']}, inner_transfer: {self.progress['inner_transfer']}"
                    f", cross_call: {self.progress['cross_call']}, inner_call: {self.progress['inner_call']}")
            if block_num % 1000 == 0:
                self.progress['block_num'] = block_num
                self.save_progress()
            if block_num == self.to_block - 1:
                self.progress['finish'] = True
                self.save_progress()

    def is_conflict(self, tx):
        for index, sd in tx['sds'].iterrows():
            key = f"{sd['address']}:{sd['state_addr']}"
            if key in self.states2num2commit:
                return True
        return False

    def cnt_conflict_addr(self, tx):
        for index, sd in tx['sds'].iterrows():
            key = f"{sd['address']}:{sd['state_addr']}"
            if key in self.states2num2commit:
                self.conflict_addr2cnt[key] += 1

    def cross_call_cnt(self, tx):
        cross_call_cnt = 0
        if self.cross_shard(tx['tx']['from'], tx['tx']['to']):
            cross_call_cnt += 1
        itxs = tx['itxs']
        if itxs is not None:
            for index, itx in itxs.iterrows():
                if self.cross_shard(itx['from'], itx['to']):
                    cross_call_cnt += 1
        return cross_call_cnt

    def save_uncommit_tx(self, tx, current_block_num) -> None:
        # 计算跨分片调用次数
        cross_call_cnt = tx['cross_call_cnt']
        tx_hash = tx['tx']['transactionHash']
        tx_obj = tx['tx'].to_dict()
        tx_obj['cross_call_cnt'] = cross_call_cnt

        # 对状态上锁，并且解锁时间为调用次数+1个之后
        if cross_call_cnt > 0:
            if self.unlock_strategy == self.NORMAL_UNLOCK_STRATEGY:
                num2commit = current_block_num + cross_call_cnt
                for index, sd in tx['sds'].iterrows():
                    key = f"{sd['address']}:{sd['state_addr']}"
                    self.states2num2commit[key] = num2commit
                    self.num2unlock_state[num2commit][key] = True
                    self.uncommitted_txs[tx_hash] = tx_obj
                    self.num2commit_tx_hash[num2commit][tx_hash] = True
            if self.unlock_strategy == self.SSC_UNLOCK_STRATEGY:
                # 找出所有冲突的交易，该交易只能在最晚提交的冲突交易的下一个区块进行提交
                conflict_tx_commit_num = current_block_num
                for index, sd in tx['sds'].iterrows():
                    key = f"{sd['address']}:{sd['state_addr']}"
                    if key in self.states2num2commit:
                        conflict_tx_commit_num = max(conflict_tx_commit_num, self.states2num2commit[key],
                                                     current_block_num + cross_call_cnt)
                num2commit = conflict_tx_commit_num + 1

                # 如果是delay-to-fail,判断能否在活性窗口之前完成提交
                if self.conflict_strategy == self.DELAY_TO_FAIL_STRATEGY and num2commit - current_block_num > self.max_delay:
                    self.progress['fail'] += 1
                    return

                for index, sd in tx['sds'].iterrows():
                    key = f"{sd['address']}:{sd['state_addr']}"
                    # 去除原本要提交的状态，延后到num2commit
                    if key in self.states2num2commit:
                        old_num2commit = self.states2num2commit[key]
                        del self.num2unlock_state[old_num2commit][key]
                    self.states2num2commit[key] = num2commit
                    self.num2unlock_state[num2commit][key] = True
                    self.uncommitted_txs[tx_hash] = tx_obj
                    self.num2commit_tx_hash[num2commit][tx_hash] = True

    def clean_uncommit_tx(self, block_number: int, commit_timestamp: int) -> None:
        unlock_state = self.num2unlock_state[block_number]
        for key in unlock_state:
            del self.states2num2commit[key]
        del self.num2unlock_state[block_number]
        self.progress['ctx_commit_cnt'] += len(self.num2commit_tx_hash[block_number])
        for tx_hash in self.num2commit_tx_hash[block_number]:
            tx = self.uncommitted_txs[tx_hash]
            commit_delay = block_number - tx['blockNumber']
            cross_call_cnt = tx['cross_call_cnt']
            if cross_call_cnt not in unlock_state:
                self.matrix[commit_delay][cross_call_cnt] = 0
            self.matrix[commit_delay][cross_call_cnt] += 1
            self.progress['delay_cnt'] += commit_delay
            self.delays.append(commit_delay)
            self.delay_times.append(commit_timestamp - tx['timestamp'])
            del self.uncommitted_txs[tx_hash]
        del self.num2commit_tx_hash[block_number]

    def calc_conflict_txs(self) -> None:
        if self.progress['finish']:
            return

        cur_tx_block_number = 0
        cur_txs = None
        cur_itx_block_number = 0
        cur_itxs = None

        for block_num, sds_df in self.sd_gen:
            if block_num < self.progress['block_num']:
                if block_num % 100 == 0:
                    print(f"skip: [{block_num}/21000000]")
                continue
            # 取得同一区块对应的state_diff, txs, itxs
            while cur_tx_block_number < block_num:
                cur_tx_block_number, cur_txs = next(self.tx_gen)
            while cur_itx_block_number < block_num:
                cur_itx_block_number, cur_itxs = next(self.itx_gen)

            txs = []
            txs_groups = cur_txs.groupby('transactionHash', sort=False)
            itxs_groups = cur_itxs.groupby('transactionHash', sort=False)

            # 循环处理所有外部交易
            for tx_hash, tx_sds in sds_df.groupby('tx_hash', sort=False):
                txs_group = txs_groups.get_group(tx_hash).iloc[0]
                itxs_group = itxs_groups.get_group(tx_hash) if tx_hash in itxs_groups.groups else None
                tx = {'tx': txs_group, 'sds': tx_sds, 'itxs': itxs_group}
                txs.append(tx)

            commit_timestamp = txs[0]['tx']['timestamp']
            # 我们会优先执行提交相关的交易，先对当前区块要提交的交易进行解锁
            self.clean_uncommit_tx(block_num, commit_timestamp)

            self.progress['tx_cnt'] += len(txs)
            for tx in txs:
                cross_call_cnt = self.cross_call_cnt(tx)
                if cross_call_cnt > 0:
                    self.progress['ctx_cnt'] += 1
                tx['cross_call_cnt'] = cross_call_cnt
                # 判断该交易是否与其他交易产生冲突
                conflict = self.is_conflict(tx)
                if conflict:
                    self.progress['conflict'] += 1
                    self.cnt_conflict_addr(tx)
                    if self.conflict_strategy == self.FAIL_CONFLICT_STRATEGY:
                        self.progress['fail'] += 1
                # 如果冲突并且采用冲突即失败的策略
                if conflict and self.conflict_strategy == self.FAIL_CONFLICT_STRATEGY:
                    continue
                # 对于原方案，必定只能直接失败
                if conflict and self.unlock_strategy == self.NORMAL_UNLOCK_STRATEGY:
                    continue
                # 其他情况都可以去计算状态什么时候可以提交
                self.save_uncommit_tx(tx, block_num)

            if block_num % 1 == 0:
                print(
                    f"complete: [{block_num}/{self.to_block}], tx_cnt: {self.progress['tx_cnt']}, ctx_cnt: {self.progress['ctx_cnt']}, "
                    f"ctx_commit_cnt: {self.progress['ctx_commit_cnt']}, conflict: {self.progress['conflict']}, fail: {self.progress['fail']}, "
                    f"delay_cnt: {self.progress['delay_cnt']}, delay_avg: {self.progress['delay_cnt'] / max(self.progress['ctx_commit_cnt'],1)}")
            if block_num % 100 == 0:
                self.progress['block_num'] = block_num
                self.save_progress()

        self.progress['finish'] = True
        self.save_progress()
        return

    def form_simulate_data(self):
        data = []
        cur_tx_block_number = 0
        cur_txs = None
        cur_itx_block_number = 0
        cur_itxs = None
        chunk_size = 100

        for block_num, sds_df in self.sd_gen:
            print("block_num:", block_num)
            if block_num >= self.to_block:
                break
            # 取得同一区块对应的state_diff, txs, itxs
            while cur_tx_block_number < block_num:
                cur_tx_block_number, cur_txs = next(self.tx_gen)
            while cur_itx_block_number < block_num:
                cur_itx_block_number, cur_itxs = next(self.itx_gen)
            txs = []
            txs_groups = cur_txs.groupby('transactionHash', sort=False)
            itxs_groups = cur_itxs.groupby('transactionHash', sort=False)

            # 循环处理所有外部交易
            for tx_hash, tx_sds in sds_df.groupby('tx_hash', sort=False):
                txs_group = txs_groups.get_group(tx_hash).iloc[0]
                itxs_group = itxs_groups.get_group(tx_hash) if tx_hash in itxs_groups.groups else None
                tx = {'tx': txs_group, 'sds': tx_sds, 'itxs': itxs_group}
                txs.append(tx)

            for tx in txs:
                # 如果是创建合同，目标地址为0,跳过
                if self.ignore_addr(tx['tx']['to']):
                    print("如果是创建合同，目标地址为0,跳过")
                    continue
                if tx['itxs'] is not None and len(tx['itxs']) >= 100:
                    print("链式调用超过100,忽略该交易")
                    continue
                data.append(self.form_simulate_data_from_tx(tx))
            if (block_num+1) % chunk_size == 0:
                df = pd.DataFrame(data)
                df.to_csv(f"simulate_data/{block_num-chunk_size + 1}-{block_num}.csv", index=False)
                data = []

        # df = pd.DataFrame(data)
        # df.to_csv("simulate_data.csv")

    def form_simulate_data_from_tx(self, tx) -> dict:
        arg1 = []
        sds_df = tx['sds']
        parent_addr = tx['tx']['to']
        shard_id = self.get_shard(parent_addr)
        cross_call_cnt = self.cross_call_cnt(tx)
        internal_txs = [{'from': tx['tx']['from'], 'to': tx['tx']['to'], 'parent_index': 0, 'index': 0, 'depth': 0, "shard_id": shard_id}]
        executed = defaultdict(bool)
        counter = [0]
        if tx['itxs'] is not None:
            self.build_call_tree(internal_txs, tx['itxs'], executed, counter, parent_addr)
        for internal_tx in internal_txs:
            internal_tx['states'] = []
            for _, sds in (sds_df[sds_df['address']==internal_tx['to']]).iterrows():
                internal_tx['states'].append(f"{sds['address']}:{sds['state_addr']}")
            sds_df = sds_df[sds_df['address']!=internal_tx['to']]
            arg1.append({
                'from': internal_tx['from'],
                'to': internal_tx['to'],
                'parent_index': internal_tx['parent_index'],
                'index': internal_tx['index'],
                'shard_id': internal_tx['shard_id'],
                'states': internal_tx['states'],
            })
        tx_args = []
        tx_args.append(arg1)
        return {
            'shard_id': shard_id,
            'block_number': tx['tx']['blockNumber'],
            'tx_hash': tx['tx']['transactionHash'],
            'from_addr': tx['tx']['from'],
            'to_addr': tx['tx']['to'],
            'cross_shard': cross_call_cnt > 0,
            'tx_args': json.dumps(tx_args),
            'cross_cnt': cross_call_cnt,
        }

    def build_call_tree(self, internal_txs, itxs, executed, counter, current_from=None, current_depth=1, parent_index=0):
        tree = []
        parent_index = counter[0]
        children_calls = [(i, t) for i, t in itxs.iterrows() if t['from'] == current_from]
        for (i, call) in children_calls:
            if executed[i]:
                continue
            counter[0] += 1
            index = counter[0]
            if self.ignore_addr(call['to']):
                print("如果是创建合同，目标地址为0,跳过")
                continue
            shard_id = self.get_shard(call['to'])
            node = {
                'from': call['from'],
                'to': call['to'],
                'depth': current_depth,
                'index': index,
                'parent_index': parent_index,
                'shard_id': shard_id,
            }
            tree.append(node)
            internal_txs.append(node)
            executed[i] = True
            self.build_call_tree(internal_txs, itxs, executed, counter, call['to'], current_depth + 1, parent_index=index)

    def to_tx_cnt_data(self):
        return {
            "tx_cnt": self.progress['tx_cnt'],
            "ctx_cnt": self.progress['ctx_cnt'],
            "ctx_commit_cnt": self.progress['ctx_commit_cnt'],
            "conflict": self.progress['conflict'],
            "fail": self.progress['fail'],
        }

    def to_delay_data(self):
        return {
            "delay_avg": self.progress['delay_cnt'] / max(self.progress['ctx_commit_cnt'],1),
            "delays": self.delays,
            "delay_times": self.delay_times,
        }


tx_files = ['../on_chain/data/20000000to20249999_BlockTransaction.csv',
            '../on_chain/data/20250000to20499999_BlockTransaction.csv',
            '../on_chain/data/20500000to20749999_BlockTransaction.csv',
            '../on_chain/data/20750000to20999999_BlockTransaction.csv']
itx_files = ['../on_chain/data/20000000to20249999_InternalTransaction.csv',
             '../on_chain/data/20250000to20499999_InternalTransaction.csv',
             '../on_chain/data/20500000to20749999_InternalTransaction.csv',
             '../on_chain/data/20750000to20999999_InternalTransaction.csv']
sd_files = [f'data/storage_2000{i}000_2000{i}999.csv' for i in range(10)]
batches = {}
data = {}
for ss in ["prefix_2"]:
    for name, us, cs, d in [
        (f"normal_{ss.replace('prefix_', '')}", StateDiffProcessor.NORMAL_UNLOCK_STRATEGY, StateDiffProcessor.FAIL_CONFLICT_STRATEGY, 0),
        (f"ssc_{ss.replace('prefix_', '')}:1", StateDiffProcessor.SSC_UNLOCK_STRATEGY, StateDiffProcessor.FAIL_CONFLICT_STRATEGY, 0),
        (f"ssc_{ss.replace('prefix_', '')}:5", StateDiffProcessor.SSC_UNLOCK_STRATEGY, StateDiffProcessor.DELAY_TO_FAIL_STRATEGY, 5),
        (f"ssc_{ss.replace('prefix_', '')}:10", StateDiffProcessor.SSC_UNLOCK_STRATEGY, StateDiffProcessor.DELAY_TO_FAIL_STRATEGY, 10),
        (f"ssc_{ss.replace('prefix_', '')}:15", StateDiffProcessor.SSC_UNLOCK_STRATEGY, StateDiffProcessor.DELAY_TO_FAIL_STRATEGY, 15),
        (f"ssc_{ss.replace('prefix_', '')}:20", StateDiffProcessor.SSC_UNLOCK_STRATEGY, StateDiffProcessor.DELAY_TO_FAIL_STRATEGY, 20),
    ]:
        batches[name] = (ss, us, cs, d)


if __name__ == "__main__":
    # for name, batch in batches.items():
    #     ss, us, cs, d = batch
    #     processor = StateDiffProcessor(20000000, 20010000, tx_files, itx_files, sd_files,
    #                                    chunk_size=10000, shard_strategy=ss, unlock_strategy=us,
    #                                    conflict_strategy=cs, max_delay=d)
    #     processor.calc_conflict_txs()
    processor = StateDiffProcessor(20000000, 20010000, tx_files, itx_files, sd_files, shard_strategy="prefix_2")
    processor.form_simulate_data()