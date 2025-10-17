import pandas as pd
import pickle
import os
from collections import defaultdict
import dask.dataframe as dd

def save_progress(data, file_name):
    print(f"save progress to {file_name}")
    with open(file_name, 'wb') as f:
        pickle.dump(data, f)


def load_progress(file_name):
    print(f"load progress from {file_name}")
    with open(file_name, 'rb') as f:
        return pickle.load(f)


def block_generator(files, chunksize=1000):
    cache = None
    current_block = None

    for csv_path in files:
        # 使用迭代器逐块读取CSV，避免一次性加载
        reader = pd.read_csv(csv_path, chunksize=chunksize)
        for chunk in reader:
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


# 计算交易调用次数频率
def calc_call_chain(tx_files, itx_files):
    cache_file = 'calc_call_chain_progress.pkl'
    tx_gen = block_generator(tx_files)
    itx_gen = block_generator(itx_files)

    if cache_file in os.listdir('.'):
        progress = load_progress(cache_file)
    else:
        progress = {
            'block_num': 0,
            'chain_count': defaultdict(int),
        }
    cur_itxs = None
    cur_block_number = 0

    for block_num, txs_df in tx_gen:
        if block_num < progress['block_num']:
            if block_num % 100 == 0:
                print(f"skip: [{block_num}/21000000]")
            continue
        while cur_block_number < block_num:
            cur_block_number, cur_itxs = next(itx_gen)
        # 存在内部交易
        if cur_block_number == block_num:
            tx_map = {}
            for _, tx in txs_df.iterrows():
                tx_map[tx['transactionHash']] = {
                    'transactionHash': tx['transactionHash'],
                    'from': tx['from'],
                    'to': tx['to'],
                    'call_count': 0,
                }
            for _, itx in cur_itxs.iterrows():
                if itx['transactionHash'] in tx_map:
                    tx_map[itx['transactionHash']]['call_count'] += 1
            for _, tx in tx_map.items():
                progress['chain_count'][tx['call_count']] += 1
        # 若不存在内部交易
        else:
            progress['chain_count'][0] += len(txs_df)
        if block_num % 10 == 0:
            print(f"complete: [{block_num}/21000000], chain_count: [0: {progress['chain_count'][0]}, 1: {progress['chain_count'][1]}, 2: {progress['chain_count'][2]}, 3: {progress['chain_count'][3]}]")
        if block_num % 1000 == 0:
            progress['block_num'] = block_num
            save_progress(progress, cache_file)
    print(progress['chain_count'])


def get_shard(addr):
    return int(addr, 16) % 4


def average_and_sum_call_chain(chain_count):
    total = 0
    cnt = 0
    for k, v in chain_count.items():
        total += k * v
        cnt += v
    return total/cnt, total


def calc_cross_call_chain(tx_files, itx_files):
    tx_gen = block_generator(tx_files)
    itx_gen = block_generator(itx_files)

    chain_count = defaultdict(int)
    cross_chain_count = defaultdict(int)
    cross_cnt = 0
    inner_cnt = 0
    cur_itxs = None
    cur_block_number = 0

    for block_num, txs_df in tx_gen:
        if cur_block_number < block_num:
            cur_block_number, cur_itxs = next(itx_gen)
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
                    if get_shard(itx['from']) != get_shard(itx['to']):
                        tx_map[itx['transactionHash']]['cross_call_count'] += 1
                        tx_map[itx['transactionHash']]['cross-shard'] = True
            for _, tx in tx_map.items():
                chain_count[tx['call_count']] += 1
                cross_chain_count[tx['cross_call_count']] += 1
                if tx['cross-shard']:
                    cross_cnt += 1
                else:
                    inner_cnt += 1
        # 若不存在内部交易
        else:
            for _, tx in txs_df.iterrows():
                if get_shard(tx['from']) != get_shard(tx['to']):
                    cross_chain_count[0] += 1
                    cross_cnt += 1
                else:
                    chain_count[0] += 1
        print(f"complete: [{block_num}/21000000], chain_count: {chain_count}")
    print(chain_count)


tx_files = ['data/20000000to20249999_BlockTransaction.csv', 'data/20250000to20499999_BlockTransaction.csv', 'data/20500000to20749999_BlockTransaction.csv', 'data/20750000to20999999_BlockTransaction.csv']
itx_files = ['data/20000000to20249999_InternalTransaction.csv', 'data/20250000to20499999_InternalTransaction.csv', 'data/20500000to20749999_InternalTransaction.csv', 'data/20750000to20999999_InternalTransaction.csv']
calc_call_chain(tx_files, itx_files)
