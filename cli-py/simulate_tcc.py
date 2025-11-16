import json
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
from limits import RateLimitItemPerSecond
from limits.storage import MemoryStorage
from limits.strategies import MovingWindowRateLimiter

from sign import SigningService

from pyhmy import transaction
from eth_utils.curried import keccak
from web3 import Web3
import pandas as pd
import logging

import visual

hash = keccak('crossCnt'.encode("utf-8"))

load_dotenv()

class HmyTCCSimulator:
    def __init__(self):
        self.env = os.getenv("ENV")
        self.tx_num = int(os.getenv("TX_NUM"))
        self.shard_num = int(os.getenv("SHARD_NUM", 4))
        self.path = "../../harmony/.hmy/expr_accounts"
        self.accounts = defaultdict(list)
        self.account_generators = {}
        self.sign_service = SigningService(self.path, max_signer_workers=os.cpu_count())
        self.txs = pd.read_csv("../data/simulate_data.csv")
        with open("../abi/bytecodes.json", 'r') as fp:
            bytecodes = json.load(fp)
            self.contracts = bytecodes["addresses"][str(self.shard_num)]
        with open("../abi/tcc_abi.json", 'r') as fp:
            abi = json.load(fp)
            w3 = Web3()
            self.contract = w3.eth.contract(abi=abi)
        self.events_type2topic = {
            "lock": keccak("KeysLocked(bytes32[],address)".encode("utf-8")).hex(),
            "unlock": keccak("KeysUnlocked(bytes32[],address)".encode("utf-8")).hex(),
            "lock_error": keccak("LockError(bytes32,string)".encode("utf-8")).hex(),
        }
        self.events_topic2type = {
            "0x" + keccak("KeysLocked(bytes32[],address)".encode("utf-8")).hex(): "lock",
            "0x" + keccak("KeysUnlocked(bytes32[],address)".encode("utf-8")).hex(): "unlock",
            "0x" + keccak("LockError(bytes32,string)".encode("utf-8")).hex(): "lock_error",
        }
        self.nonce_lock = threading.Lock()
        self.wait_lock = threading.Lock()
        self.result_lock = threading.Lock()
        self.logger = self.setup_logger()
        self.logger.info("start")

        self.results = []
        self.result_cnt = 0
        self.sub_txs = []
        self.last_saved_count = 0
        self.saved_batches = set()
        self.target_path = "./"
        result_dir = os.path.join(self.target_path, "result")
        os.makedirs(result_dir, exist_ok=True)
        max_tx_workers = 1000
        self.timeout=60
        self.tx_executor = ThreadPoolExecutor(max_workers=max_tx_workers, thread_name_prefix="TxWorker")
        RATE_LIMIT = int(os.getenv("RATE_LIMIT", "100"))  # 每秒最多交易数，默认50
        self.storage = MemoryStorage()
        self.rate_limiter = MovingWindowRateLimiter(self.storage)
        self.rate_item = RateLimitItemPerSecond(RATE_LIMIT, 1)

    def setup_logger(self, log_file='app.log', console_level=logging.INFO, file_level=logging.INFO):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)  # 设置最低级别

        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # 文件handler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)

        # 添加handler
        logger.addHandler(file_handler)

        return logger

    def encode_abi(self, fn_name, args):
        data = self.contract.encodeABI(fn_name, kwargs=args)
        return data

    def get_endpoint(self, shard_id) -> str:
        if self.env == "local":
            return f"http://127.0.0.1:{9500 + 40 * shard_id}"
        if self.env == "dev":
            return f"http://10.7.95.{shard_id+200}:{9500 + 40 * shard_id}"
        return "http://127.0.0.1:9500"

    def execute_cross_tx(self, index, index_shard, tx_tuple):
        while not self.rate_limiter.hit(self.rate_item):
            time.sleep(0.001)  # 短暂等待后重试（避免忙等）

        shard_id = tx_tuple.shard_id
        to_addr = self.contracts[shard_id]
        json_args = json.loads(tx_tuple.tx_args.replace("'", '"'))
        sub_txs = []
        state_map = {}
        last_shard = -1
        states = []
        cnt = 0
        start_block = 0
        for arg in json_args[0]:
            sub_tx_shard_id = arg["shard_id"]
            for state in arg["states"]:
                key = keccak(state.encode('utf-8'))
                if key not in state_map:
                    states.append(key)
                    state_map[key] = True
            if last_shard != sub_tx_shard_id:
                sub_tx = {}
                sub_tx["index"] = cnt
                sub_tx["internal_index"] = arg["index"]
                sub_tx["shard_id"] = arg["shard_id"]
                sub_tx["states"] = states
                sub_tx["tx_args"] = self.encode_abi("lock", {"keys": states})
                sub_txs.append(sub_tx)
                states = []
                last_shard = shard_id
                cnt += 1
        locked_states = []
        commit = True
        try:
            try:
                for sub_tx in sub_txs:
                    data = sub_tx["tx_args"]
                    local_net = self.get_endpoint(shard_id)
                    tx = {
                        'chainId': 2,
                        'gas': 5000000,
                        'gasPrice': 3000000000,
                        'shardID': shard_id,
                        'to': to_addr,
                        'toShardID': shard_id,
                        'crossShard': False,
                        'value': 0,
                        'data': data
                    }
                    future = self.sign_service.submit_signing_request(tx, shard_id)
                    signed_tx = future.result()
                    nonce = tx["nonce"]
                    if start_block == 0:
                        tx_start_time = time.time()
                    tx_info = transaction.send_and_confirm_raw_transaction(
                        signed_tx.rawTransaction.hex(), local_net, self.timeout)
                    receipt = transaction.get_transaction_receipt(tx_info['hash'], local_net, timeout=self.timeout)
                    event_type = self.handle_receipt(receipt)
                    if start_block == 0:
                        start_block = tx_info['blockNumber']
                    with self.result_lock:
                        self.sub_txs[index].append({
                            "tx_index": index,
                            "index": sub_tx["index"],
                            "indexShard": index_shard,
                            "shard": shard_id,
                            "nonce": nonce,
                            "blockNumber": tx_info['blockNumber'],
                            "time": time.time() - tx_start_time,
                        })
                    if event_type == "lock":
                        self.logger.info(f"sub_tx finished: [{index}/{sub_tx['index']+1}/{len(sub_txs)}], shard={shard_id}, nonce={nonce}, blockNum=[{start_block}->{tx_info['blockNumber']}], time={time.time()-tx_start_time}s")
                        locked_states.append(signed_tx.hash.hex())
                    elif event_type == "error":
                        self.logger.error(f"error: : [{sub_tx['index']+1}/{len(sub_txs)}], shard={shard_id}, nonce={nonce}, blockNum=[{start_block}->{tx_info['blockNumber']}], time={time.time()-tx_start_time}s")
                    else:
                        self.logger.info(f"lock error, rollback transaction: [{sub_tx['index']+1}/{len(sub_txs)}], shard={shard_id}, nonce={nonce}, blockNum=[{start_block}->{tx_info['blockNumber']}], time={time.time()-tx_start_time}s")
                        commit = False
                        break
            except Exception as e:
                self.logger.error(f"exception: {e}, rollback transaction: [{index}/{sub_tx['index']+1}/{len(sub_txs)}], shard={shard_id}, time={time.time()-tx_start_time}s")
                commit = False
            finally:
                local_net = self.get_endpoint(shard_id)
                data = self.contract.encodeABI("unlock", {"keys": locked_states})
                unlock_tx = {
                    'chainId': 2,
                    'gas': 50000,
                    'gasPrice': 3000000000,
                    'shardID': shard_id,
                    'to': to_addr,
                    'toShardID': shard_id,
                    'crossShard': False,
                    'value': 0,
                    'data': data
                }
                future = self.sign_service.submit_signing_request(index, unlock_tx, shard_id)
                signed_tx = future.result()
                tx_info = transaction.send_and_confirm_raw_transaction(
                    signed_tx.rawTransaction.hex(), local_net, timeout=self.timeout)
                end_block = tx_info["blockNumber"]
                if commit:
                    status = "completed"
                else:
                    status = "error"
        except Exception as e:
            status = "error"
            tx_end_time = time.time()
            end_block = start_block
            self.logger.error(f"exception: {e}, cross tx failed: index={index}, shard={shard_id}:{index_shard}, time={tx_end_time - tx_start_time}s")
        finally:
            tx_end_time = time.time()
            self.logger.info(f"cross tx finished: index={index}, shard={shard_id}:{index_shard}, commit={commit}, cross_cnt={len(sub_txs)}, time={tx_end_time - tx_start_time}s")
            with self.result_lock:
                self.results[index]['index_shard'] = index_shard
                self.results[index]['commit'] = commit
                self.results[index]['time'] = tx_end_time - tx_start_time
                self.results[index]['start_time'] = tx_start_time
                self.results[index]['end_time'] = tx_end_time
                self.results[index]['cross_cnt'] = len(sub_txs)
                self.results[index]['start_block'] = start_block
                self.results[index]['end_block'] = end_block
                self.result_cnt += 1
                if self.result_cnt % 1000 == 0:
                    self.save_results()

    def save_results(self):
        df = pd.DataFrame(self.results)
        result_dir = os.path.join(self.target_path, "result")
        results_file = os.path.join(result_dir, "simulate_tcc_results.csv")
        df.to_csv(results_file)

    def handle_receipt(self, receipt) -> str:
        logs = receipt['logs']
        if len(logs) == 0:
            return "error"
        log = logs[0]
        if len(log['topics']) == 0:
            return "error"
        topic = log['topics'][0]
        if not topic in self.events_topic2type:
            return "error"
        return self.events_topic2type[topic]

    def save_final_state(self):
        """保存最终状态和统计信息"""
        try:
            result_dir = os.path.join(self.target_path, "result")

            # 保存统计信息
            stats = {
                'total_transactions': len(self.results),
                'completed_transactions': sum(1 for r in self.results if r.get('commit', False)),
                'failed_transactions': sum(1 for r in self.results if not r.get('commit', True)),
                'total_time': max(r.get('end_time', 0) for r in self.results) - min(
                    r.get('start_time', 0) for r in self.results),
                'completion_time': time.time()
            }

            with open(os.path.join(result_dir, "final_stats.json"), 'w') as f:
                json.dump(stats, f, indent=2)

            self.logger.info(f"最终统计: {stats}")

        except Exception as e:
            self.logger.error(f"保存最终状态时发生错误: {e}")

    def test(self):
        try:
            cnt = 0
            shard_cnts = [0] * len(self.contracts)
            futures = []
            max_workers = min(32, os.cpu_count() + 4)  # 合理设置线程数
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for tx_tuple in self.txs.itertuples():
                    if cnt >= self.tx_num:
                        break
                    shard_id = tx_tuple.shard_id
                    future = self.tx_executor.submit(self.execute_cross_tx, cnt, shard_cnts[shard_id], tx_tuple,)
                    with self.result_lock:
                        self.results.append({"index": cnt, "shard": shard_id})
                        self.sub_txs.append([])
                    futures.append(future)
                    cnt += 1
                    shard_cnts[shard_id] += 1
                for index, future in enumerate(futures):
                    self.logger.info(f"begin to wait for thread {index}")
                    future.result()
                for shard_id, shard_cnt in enumerate(shard_cnts):
                    self.logger.info(f"shard {shard_id}:{shard_cnt}/{cnt}")
        finally:
            self.save_results()
            self.save_final_state()
            self.logger.info(f"测试完成，共处理 {self.result_cnt} 个交易")


simulator = HmyTCCSimulator()
simulator.test()
