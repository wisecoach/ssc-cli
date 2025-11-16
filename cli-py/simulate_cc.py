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

class HmyCCSimulator:
    def __init__(self):
        self.env = os.getenv("ENV")
        self.tx_num = int(os.getenv("TX_NUM"))
        self.shard_num = int(os.getenv("SHARD_NUM", 4))
        self.path = "../../cc-harmony/.hmy/expr_accounts"
        self.accounts = defaultdict(list)
        self.account_generators = {}
        self.sign_service = SigningService(self.path, max_signer_workers=os.cpu_count())
        self.txs = pd.read_csv("../data/simulate_data.csv")
        with open("../abi/bytecodes.json", 'r') as fp:
            bytecodes = json.load(fp)
            self.contracts = bytecodes["addresses"][str(self.shard_num)]
        with open("../abi/cc_abi.json", 'r') as fp:
            abi = json.load(fp)
            w3 = Web3()
            self.contract = w3.eth.contract(abi=abi)
        RATE_LIMIT = int(os.getenv("RATE_LIMIT", "100"))  # 每秒最多交易数，默认50
        self._rate_storage = MemoryStorage(enable_threading=True)
        self.rate_limiter = MovingWindowRateLimiter(self._rate_storage)
        self.rate_item = RateLimitItemPerSecond(RATE_LIMIT, 1)
        self.rate_limiter.hit(self.rate_item)  # 第一次调用会初始化内部状态
        print(f"simulator config: tx_num={self.tx_num}, rate_limit={RATE_LIMIT}/s, env={self.env}")

    def encode_abi(self, fn_name, args):
        data = self.contract.encodeABI(fn_name, kwargs=args)
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

        n = len(indexes)
        if n == 0:
            subtreeSizes = []
            maxDepthIndex = 0
        else:
            # Step 1: 构建 index -> position 映射
            index_to_pos = {}
            for pos, idx_val in enumerate(indexes):
                index_to_pos[idx_val] = pos

            # Step 2: 初始化 subtreeSizes 为 1
            subtreeSizes = [1] * n

            parentIndexes[0] = -1  # 根节点的 parent_index 设置为 -1

            # Step 3: 构建子节点关系映射
            children_map = {}
            for i in range(n):
                parent_val = parentIndexes[i]
                if parent_val != -1:
                    if parent_val not in children_map:
                        children_map[parent_val] = []
                    children_map[parent_val].append(indexes[i])

            # Step 4: 计算子树大小（从后往前遍历）
            for i in range(n - 1, -1, -1):
                parent_val = parentIndexes[i]
                if parent_val != -1:
                    if parent_val in index_to_pos:
                        parent_pos = index_to_pos[parent_val]
                        subtreeSizes[parent_pos] += subtreeSizes[i]
                    else:
                        raise ValueError(f"Parent index {parent_val} not found in indexes")

            # Step 5: 计算每个节点的深度并找到最深的跨分片调用节点
            node_depths = [0] * n
            max_cross_shard_depth = 0
            max_depth_index = 0

            # 使用DFS计算深度，只考虑跨分片调用
            def calculate_cross_shard_depth(node_index, current_depth, current_shard_id):
                nonlocal max_cross_shard_depth, max_depth_index
                pos = index_to_pos[node_index]
                node_shard_id = shardIds[pos]

                # 如果是跨分片调用，更新深度
                if node_shard_id != current_shard_id:
                    current_depth += 1
                    node_depths[pos] = current_depth

                    # 更新最大跨分片深度
                    if current_depth > max_cross_shard_depth:
                        max_cross_shard_depth = current_depth
                        max_depth_index = node_index
                    elif current_depth == max_cross_shard_depth and node_index > max_depth_index:
                        # 相同深度时选择索引更大的节点
                        max_depth_index = node_index
                else:
                    # 分片内调用，深度不变
                    node_depths[pos] = current_depth

                # 递归计算子节点深度
                if node_index in children_map:
                    for child_index in children_map[node_index]:
                        child_pos = index_to_pos[child_index]
                        calculate_cross_shard_depth(child_index, current_depth, node_shard_id)

            # 从根节点开始计算深度
            root_index = indexes[0]
            root_shard_id = shardIds[0]
            calculate_cross_shard_depth(root_index, 0, root_shard_id)

            # 如果没有跨分片调用，选择根节点
            if max_cross_shard_depth == 0:
                maxDepthIndex = root_index
            else:
                maxDepthIndex = max_depth_index

        args['index'] = 0
        args['indexes'] = indexes
        args['shardIds'] = shardIds
        args['statesCounts'] = statesCounts
        args['statesHashes'] = statesHashes
        args['subtreeSizes'] = subtreeSizes
        args['maxDepthIndex'] = maxDepthIndex

        return args

    def get_endpoint(self, shard_id) -> str:
        if self.env == "local":
            return f"http://127.0.0.1:{9500 + 40 * shard_id}"
        if self.env == "dev":
            return f"http://10.7.95.{shard_id+200}:{9500 + 40 * shard_id}"
        return "http://127.0.0.1:9500"

    def execute_cross_tx(self, index, shard_cnt, tx_tuple):
        while not self.rate_limiter.hit(self.rate_item):
            time.sleep(0.001)

        shard_id = tx_tuple.shard_id
        to_addr = self.contracts[shard_id]
        cross_shard = tx_tuple.cross_shard
        tx_args = self.resolve_args(tx_tuple.tx_args)
        tx_args["txId"] = index
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
            'value': 50000000 * 3000000000,
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
        max_workers = min(32, os.cpu_count())  # 合理设置线程数
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

        for shard_id, shard_cnt in enumerate(shard_cnts):
            print(f"shard {shard_id}:{shard_cnt}/{cnt}")

        # 可选：等待所有完成并收集异常
        for future in as_completed(futures):
            future.result()


simulator = HmyCCSimulator()
simulator.execute()
