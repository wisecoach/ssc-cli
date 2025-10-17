import copy
import os
import time

from pyhmy import signing
from pyhmy import transaction
from eth_utils.curried import keccak
from concurrent.futures import ThreadPoolExecutor

hash = keccak('crossCnt'.encode("utf-8"))
path = "../harmony/.hmy/expr_accounts"
local_net = f'http://localhost:9500'
transfer_addr = '0x95a759428f9a8b4bc02e20086085f32b7a440463'
cx_transfer_addr = '0x660a4de91307f84b7de28057c25135d409015f2c'

ctx_rate = 1
txn = 50
shard_num = 5

tx_template = [
    {"ss": 1, "cnt": 70},
    {"ss": 2, "cnt": 13},
    {"ss": 3, "cnt": 8},
    {"ss": 4, "cnt": 3},
    {"ss": 5, "cnt": 1},
    {"ss": 6, "cnt": 3},
    {"ss": 7, "cnt": 2},
]

# tx_template = [
#     {"ss": 3, "cnt": 1},
# ]


def get_net(shard_id):
    return f'http://localhost:{9500 + shard_id*40}'


def get_tx_template():
    return copy.deepcopy(tx_template)


def get_ss():
    current_tx_template = get_tx_template()
    while True:
        tx = current_tx_template[0]
        if tx["cnt"] > 0:
            tx["cnt"] -= 1
            yield tx["ss"]
        if tx["cnt"] == 0:
            current_tx_template.remove(tx)
        if len(current_tx_template) == 0:
            current_tx_template = get_tx_template()


def retry_operation(max_retries, delay, operation, *args, **kwargs):
    for attempt in range(max_retries):
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            print(f"尝试 {attempt + 1} 失败: {e}")
            if attempt < max_retries - 1:
                print(f"等待 {delay} 秒后重试...")
                time.sleep(delay)
            else:
                print("已达到最大重试次数。")
                raise


def send_tx(shard_id, index, sender, nonce, key_file, ss, crossShard, shard_num, contract_addr):
    with open(os.path.join(path, key_file), 'r') as fp:
        pri_key = fp.readline()
        tx = {
            'chainId': 2,
            'from': '0x' + sender,
            'gas': 5000000,
            'gasPrice': 3000000000,
            'nonce': nonce,
            'shardID': shard_id,
            'to': contract_addr,
            'toShardID': 0,
            'crossShard': crossShard,
            'value': 0,
            'data': '0x8d212193' +
                    '0000000000000000000000000000000000000000000000000000000000000000' +
                    f'000000000000000000000000000000000000000000000000000000000000000{shard_num}' +
                    f'000000000000000000000000000000000000000000000000000000000000000{ss}'
        }
        print(f"send transaction: shard_id={shard_id}, nonce={nonce}, keyfile={key_file}, index={index}")
        raw_tx = signing.sign_transaction(tx, pri_key).rawTransaction.hex()
        try:
            retry_operation(5, 0.001, transaction.send_raw_transaction, raw_tx, get_net(shard_id))
        except Exception as e:
            print(f"send transaction failed: shard_id={shard_id}, nonce={nonce}, keyfile={key_file}, index={index}")


def async_test(shard_id, thread_num=10):
    start_time = time.time()
    ss_gen = get_ss()
    with ThreadPoolExecutor(max_workers=thread_num) as executor:
        for nonce in range(txn):
            for index, key_file in enumerate(os.listdir(path)):
                ss = next(ss_gen)
                if ss > 1:
                    contract_addr = cx_transfer_addr
                    crossShard = True
                else:
                    contract_addr = transfer_addr
                    crossShard = False
                sender = key_file.replace('.key', '')
                executor.submit(send_tx, shard_id, index, sender, nonce, key_file, ss, crossShard, shard_num, contract_addr)
    print(f"async test: {time.time() - start_time}")


def sync_test(shard_id):
    start_time = time.time()
    ss_gen = get_ss()
    for nonce in range(txn):
        for index, key_file in enumerate(os.listdir(path)):
            ss = next(ss_gen)
            if ss > 1:
                contract_addr = cx_transfer_addr
                crossShard = True
            else:
                contract_addr = transfer_addr
                crossShard = False
            sender = key_file.replace('.key', '')
            send_tx(shard_id, index, sender, nonce, key_file, ss, crossShard, shard_num, contract_addr)
    print(f"sync test: {time.time() - start_time}")


# async_test(20)
# sync_test(1)
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(sync_test, i) for i in range(shard_num)]
    for future in futures:
        future.result()
