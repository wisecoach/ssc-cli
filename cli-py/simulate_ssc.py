import json
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyhmy import transaction
from eth_utils.curried import keccak
from web3 import Web3
import pandas as pd
from dotenv import load_dotenv
from sign import SigningService
from limits import RateLimitItemPerSecond
from limits.storage import MemoryStorage
from limits.strategies import MovingWindowRateLimiter

hash = keccak('crossCnt'.encode("utf-8"))

load_dotenv()

class HmySSCSimulator:
    def __init__(self):
        self.env = os.getenv("ENV")
        self.tx_num = int(os.getenv("TX_NUM"))
        self.shard_num = int(os.getenv("SHARD_NUM", 4))
        self.path = "../../ssc-harmony/.hmy/expr_accounts"
        self.accounts = defaultdict(list)
        self.account_generators = {}
        self.sign_service = SigningService(self.path, max_signer_workers=os.cpu_count())
        self.txs = pd.read_csv("../data/simulate_data.csv")
        with open("../abi/bytecodes.json", 'r') as fp:
            bytecodes = json.load(fp)
            self.contracts = bytecodes["addresses"][str(self.shard_num)]
        with open("../abi/ssc_abi.json", 'r') as fp:
            abi = json.load(fp)
            w3 = Web3()
            self.contract = w3.eth.contract(abi=abi)
        RATE_LIMIT = int(os.getenv("RATE_LIMIT", "100"))  # 每秒最多交易数，默认50
        self.storage = MemoryStorage()
        self.rate_limiter = MovingWindowRateLimiter(self.storage)
        self.rate_item = RateLimitItemPerSecond(RATE_LIMIT, 1)

    def encode_abi(self, fn_name, args):
        data = self.contract.encodeABI(fn_name, kwargs=args)
        return data

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
            return f"http://10.7.95.{shard_id+200}:{9500 + 40 * shard_id}"
        return "http://127.0.0.1:9500"

    def execute_cross_tx(self, index, shard_cnt, tx_tuple):
        # === 关键：在执行前获取速率许可 ===
        while not self.rate_limiter.hit(self.rate_item):
            time.sleep(0.001)  # 短暂等待后重试（避免忙等）

        shard_id = tx_tuple.shard_id
        to_addr = self.contracts[shard_id]
        cross_shard = tx_tuple.cross_shard
        tx_args = self.resolve_args(tx_tuple.tx_args)
        data = self.encode_abi("simulate", tx_args)
        local_net = self.get_endpoint(shard_id)
        tx = {
            'chainId': 2,
            'gas': 50000000,
            'gasPrice': 3000000000,
            'shardID': shard_id,
            'to': to_addr,
            'toShardID': shard_id,
            'crossShard': cross_shard,
            'value': 0,
            'data': data
        }
        try:
            future = self.sign_service.submit_signing_request(index, tx, shard_id)
            signed_tx = future.result()
            print(
                f"send transaction: index={index}, shard={shard_id}:{shard_cnt}, tx_hash={signed_tx.hash.hex()}")
            transaction.send_raw_transaction(
                signed_tx.rawTransaction.hex(), local_net)
        except Exception as e:
            print(
                f"send transaction failed: index={index}, shard={shard_id}:{shard_cnt}, error={e}")

    def execute(self):
        cnt = 0
        shard_cnts = [0] * len(self.contracts)
        futures = []

        # 使用线程池并发执行
        max_workers = min(32, os.cpu_count() + 4)  # 合理设置线程数
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for tx_tuple in self.txs.itertuples():
                if cnt >= self.tx_num:
                    break
                shard_id = tx_tuple.shard_id
                shard_cnt = shard_cnts[shard_id]
                # 提交任务（非阻塞）
                future = executor.submit(self.execute_cross_tx, cnt, shard_cnt, tx_tuple)
                futures.append(future)
                cnt += 1
                shard_cnts[shard_id] += 1

            # 可选：等待所有完成并收集异常
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Task raised exception: {e}")

        for shard_id, shard_cnt in enumerate(shard_cnts):
            print(f"shard {shard_id}:{shard_cnt}/{cnt}")

if __name__ == "__main__":
    simulator = HmySSCSimulator()
    simulator.execute()