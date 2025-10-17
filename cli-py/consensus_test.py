import copy
import os
import time

from pyhmy import signing
from pyhmy import transaction
from eth_utils.curried import keccak
from concurrent.futures import ThreadPoolExecutor
from web3 import Web3

hash = keccak('crossCnt'.encode("utf-8"))
path = "../harmony/.hmy/expr_accounts"
local_net = f'http://localhost:9500'

address_list = []
for index, key_file in enumerate(os.listdir(path)):
    address = key_file.replace('.key', '')
    address_list.append(address)
account_num = len(address_list)

ctx_rate = 1
txn = 50
shard_num = 5


def get_net(shard_id):
    return f'http://localhost:{9500 + shard_id * 40}'


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


def send_tx(shard_id, index, sender, target, nonce, key_file):
    with open(os.path.join(path, key_file), 'r') as fp:
        pri_key = fp.readline()
        tx = {
            'chainId': 2,
            'from': '0x' + sender,
            'gas': 5000000,
            'gasPrice': 3000000000,
            'nonce': nonce,
            'shardID': shard_id,
            'to': target,
            'toShardID': shard_id,
            'value': 0,
        }
        print(f"send transaction: shard_id={shard_id}, nonce={nonce}, keyfile={key_file}, index={index}")
        raw_tx = signing.sign_transaction(tx, pri_key).rawTransaction.hex()
        try:
            retry_operation(5, 0.001, transaction.send_raw_transaction, raw_tx, get_net(shard_id))
        except Exception as e:
            print(f"send transaction failed: shard_id={shard_id}, nonce={nonce}, keyfile={key_file}, index={index}")


def sync_test(shard_id):
    start_time = time.time()
    for nonce in range(txn):
        for index, key_file in enumerate(os.listdir(path)):
            sender = key_file.replace('.key', '')
            send_tx(shard_id, index, sender, address_list[(index + 1) % account_num], nonce, key_file)
    print(f"sync test: {time.time() - start_time}")


sync_test(0)
