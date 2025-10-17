import json
import os
import time
from collections import defaultdict

from pyhmy import signing
from pyhmy import transaction
from eth_utils.curried import keccak
from web3.contract import Contract
from web3 import Web3
import pandas as pd

hash = keccak('crossCnt'.encode("utf-8"))
ctx_rate_sum = 0
ctx_rate = 1
txn = 50
shard_num = 3
ss = 3

HEX_TO_BITS = {
    '0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7,
    '8': 8, '9': 9, 'A': 10, 'B': 11, 'C': 12, 'D': 13, 'E': 14, 'F': 15,
    'a': 10, 'b': 11, 'c': 12, 'd': 13, 'e': 14, 'f': 15
}


class HmySSCSimulator:
    def __init__(self):
        self.env = "local"
        # self.env = "dev"
        self.tx_num = 100000
        self.path = "../ssc-harmony/.hmy/expr_accounts"
        self.interval = 0.01
        self.accounts = defaultdict(list)
        self.account_generators = {}
        self.dev_ips = [
            '10.7.95.200',
            '10.7.95.201',
            '10.7.95.202',
            '10.7.95.203',
        ]
        self.contracts = [
            "0xDd66F429b278AD2076389520F4cF21e6c339d7d2",
            "0x95a759428f9a8B4bc02E20086085F32B7A440463",
            "0x660A4dE91307f84b7dE28057C25135D409015F2C",
            "0xfC7D59Be0a1B36ef7D46059DCd896252c75E57c0",
        ]
        Contract.web3 = Web3()
        Web3().eth.contract()
        Contract.abi = [
	{
		"inputs": [],
		"stateMutability": "nonpayable",
		"type": "constructor"
	},
	{
		"inputs": [
			{
				"internalType": "uint256",
				"name": "index",
				"type": "uint256"
			},
			{
				"internalType": "uint256[]",
				"name": "indexes",
				"type": "uint256[]"
			},
			{
				"internalType": "uint256[]",
				"name": "parentIndexes",
				"type": "uint256[]"
			},
			{
				"internalType": "uint256[]",
				"name": "shardIds",
				"type": "uint256[]"
			},
			{
				"internalType": "uint256[]",
				"name": "statesCounts",
				"type": "uint256[]"
			},
			{
				"internalType": "bytes32[]",
				"name": "statesHashes",
				"type": "bytes32[]"
			}
		],
		"name": "simulate",
		"outputs": [
			{
				"internalType": "uint256",
				"name": "",
				"type": "uint256"
			}
		],
		"stateMutability": "nonpayable",
		"type": "function"
	},
	{
		"inputs": [
			{
				"internalType": "bytes32",
				"name": "",
				"type": "bytes32"
			}
		],
		"name": "states",
		"outputs": [
			{
				"internalType": "uint256",
				"name": "",
				"type": "uint256"
			}
		],
		"stateMutability": "view",
		"type": "function"
	}
]
        for key_file in os.listdir(self.path):
            addr = '0x' + key_file.split(".")[0]
            shard_id = self.get_shard(addr)
            self.accounts[shard_id].append({
                "key_file": key_file,
                "address": addr,
                "shard_id": shard_id,
                "nonce": 0,
            })
        for shard_id in self.accounts.keys():
            self.account_generators[shard_id] = self.account_generator(shard_id)
        self.txs = pd.read_csv("simulate_data.csv")
        # self.txs = pd.read_csv("conflict_test_data.csv")

    def account_generator(self, shard_id):
        i = 0
        accounts = self.accounts[shard_id]
        while True:
            yield accounts[i]
            accounts[i]['nonce'] += 1
            i += 1
            if i >= len(self.accounts[shard_id]):
                i = 0

    @staticmethod
    def get_shard(addr: str, n: int = 2) -> int:
        # 直接计算掩码（避免字符串操作）
        mask = (1 << n) - 1  # 前n位的掩码，例如 n=4 时 mask=0b1111
        bits = 0
        length = 0
        # 遍历16进制字符，逐步构建二进制值
        for c in addr[2:]:
            bits = (bits << 4) | HEX_TO_BITS[c]  # 左移4位并合并新字符的4位
            length += 4
            if length >= n:  # 已累积足够的位数
                break
        # 右移多余位数并应用掩码
        shift = max(length - n, 0)
        class_num = (bits >> shift) & mask
        return class_num

    @staticmethod
    def encode_abi(fn_name, args):
        data = Contract.encodeABI(fn_name, kwargs=args)
        return data

    def select_account(self, shard_id) -> dict:
        gen = self.account_generators[shard_id]
        return next(gen)

    def resolve_args(self, tx_args) -> dict:
        args = {}
        indexes = []
        parentIndexes = []
        shardIds = []
        statesCounts = []
        statesHashes = []
        json_args = json.loads(tx_args.replace("'", '"'))
        for arg in json_args[0]:
            indexes.append(arg["index"])
            parentIndexes.append(arg["parent_index"])
            shardIds.append(arg["shard_id"])
            for state in arg["states"]:
                statesHashes.append(keccak(state.encode('utf-8')))
            statesCounts.append(len(arg['states']))
        args['indexes'] = indexes
        args['parentIndexes'] = parentIndexes
        args['shardIds'] = shardIds
        args['statesHashes'] = statesHashes
        args['statesCounts'] = statesCounts
        args['index'] = 0
        return args

    def get_endpoint(self, shard_id) -> str:
        if self.env == "local":
            return f"http://127.0.0.1:{9500 + 40 * shard_id}"
        if self.env == "dev":
            return f"http://{self.dev_ips[shard_id]}:{9500 + 40 * shard_id}"
        return "http://127.0.0.1:9500"

    def execute(self):
        tx_logs = []
        cnt = 0
        shard_cnts = [0] * len(self.contracts)
        for tx_tuple in self.txs.itertuples():
            if cnt >= self.tx_num:
                break
            shard_id = tx_tuple.shard_id
            to_addr = self.contracts[shard_id]
            cross_shard = tx_tuple.cross_shard
            tx_args = self.resolve_args(tx_tuple.tx_args)
            account = self.select_account(shard_id)
            addr = account["address"]
            nonce = account["nonce"]
            data = self.encode_abi("simulate", tx_args)
            local_net = self.get_endpoint(shard_id)
            tx = {
                'chainId': 2,
                'from': addr,
                'gas': 50000000,
                'gasPrice': 3000000000,
                'nonce': account['nonce'],
                'shardID': shard_id,
                'to': to_addr,
                'toShardID': shard_id,
                'crossShard': cross_shard,
                'value': 0,
                'data': data
            }
            with open(os.path.join(self.path, account['key_file']), "r") as fp:
                pri_key = fp.readline()
                signed_tx = signing.sign_transaction(tx, pri_key)
                print(f"send transaction: index={cnt}, shard={shard_id}:{shard_cnts[shard_id]}, nonce={nonce}, addr={addr}, tx_hash={signed_tx.hash.hex()}")
                try:
                    tx_hash = transaction.send_raw_transaction(
                        signed_tx.rawTransaction.hex(), local_net)
                    tx_logs.append({
                        "txHash": tx_hash,
                        "index": tx_tuple.index,
                        "shard": shard_id,
                        "shard_index": shard_cnts[shard_id],
                        "send_time": time.time(),
                        "cross_cnt": tx_tuple.cross_cnt
                    })
                except Exception as e:
                    print(f"send transaction failed: index={cnt}, shard={shard_id}:{shard_cnts[shard_id]}, nonce={nonce}, addr={addr}, error={e}")
                    continue
            cnt += 1
            shard_cnts[shard_id] += 1
            time.sleep(self.interval)
        for shard_id, shard_cnt in enumerate(shard_cnts):
            print(f"shard {shard_id}:{shard_cnt}/{cnt}")
        df = pd.DataFrame(tx_logs)
        df.to_csv('tx_logs.csv', index=False)


simulator = HmySSCSimulator()
simulator.execute()
