import os

from pyhmy import signing
from pyhmy import transaction
from eth_utils.curried import keccak

hash = keccak('crossCnt'.encode("utf-8"))
path = "../harmony/.hmy/expr_accounts"
local_net = f'http://localhost:9500'
transfer_addr = '0x95a759428f9a8b4bc02e20086085f32b7a440463'
cx_transfer_addr = '0x660a4de91307f84b7de28057c25135d409015f2c'

ctx_rate_sum = 0
ctx_rate = 1
txn = 50
shard_num = 3
ss = 3


for nonce in range(txn):
    for index, key_file in enumerate(os.listdir(path)):
        try:
            addr = key_file.replace('.key', '')
            with open(os.path.join(path, key_file), 'r') as fp:
                ctx_rate_sum += ctx_rate
                if ctx_rate_sum >= 1:
                    contract_addr = cx_transfer_addr
                    crossShard = True
                    ctx_rate_sum -= 1
                else:
                    contract_addr = transfer_addr
                    crossShard = False
                pri_key = fp.readline()
                tx = {
                 'chainId': 2,
                 'from': '0x' + addr,
                 'gas': 5000000,
                 'gasPrice': 3000000000,
                 'nonce': nonce,
                 'shardID': 0,
                 'to': contract_addr,
                 'toShardID': 0,
                 'crossShard': crossShard,
                 'value': 0,
                 'data': '0x8d212193' +
                         '0000000000000000000000000000000000000000000000000000000000000000' +
                         f'000000000000000000000000000000000000000000000000000000000000000{shard_num}' +
                         f'000000000000000000000000000000000000000000000000000000000000000{ss}'
                }
                print(f"send transaction: nonce={nonce}, index={index}, keyfile={key_file}")
                tx_hash = transaction.send_raw_transaction(
                    signing.sign_transaction(tx, pri_key)
                    .rawTransaction.hex(), local_net)
                print(f"send transaction success: nonce={nonce}, index={index}, keyfile={key_file}")
        except Exception:
            continue

