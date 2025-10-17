import os
import sys

from pyhmy import signing
from pyhmy import transaction
from eth_utils.curried import keccak

hash = keccak('crossCnt'.encode("utf-8"))
path = "../harmony/.hmy/expr_accounts"
local_net = 'http://localhost:9500'
transfer_addr = '0x95a759428f9a8b4bc02e20086085f32b7a440463'
cx_transfer_addr = '0x660a4de91307f84b7de28057c25135d409015f2c'

ctx_rate = 0.75
txn = 50


def transfer_test(key_file):
    ctx_rate_sum = 0
    for nonce in range(txn):
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
                         '0000000000000000000000000000000000000000000000000000000000000003' +
                         '0000000000000000000000000000000000000000000000000000000000000003'
                }
                print(f"send transaction: nonce={nonce}, keyfile={key_file}")
                tx_hash = transaction.send_raw_transaction(
                    signing.sign_transaction(tx, pri_key)
                    .rawTransaction.hex(), local_net)
                print(f"send transaction success: nonce={nonce}, keyfile={key_file}")
        except Exception:
            print(f"send transaction failed: nonce={nonce}, keyfile={key_file}")
            continue


key_file = sys.argv[1]
transfer_test(key_file)
