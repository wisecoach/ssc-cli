import heapq
import json
import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future
from logging.handlers import RotatingFileHandler

from pyhmy import signing
from pyhmy import transaction
from eth_utils.curried import keccak
from web3.contract import Contract
from web3 import Web3
import pandas as pd
import logging

import visual
from visual import ThreadMonitor

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

class SigningService:
    def __init__(self, account_generators, max_signer_workers=10):
        self.signer_executor = ThreadPoolExecutor(max_workers=max_signer_workers, thread_name_prefix="Signer")
        self.priority_queue = []
        self.queue_lock = threading.Lock()
        self.condition = threading.Condition(self.queue_lock)
        self.is_running = True
        self.account_generators = account_generators

        # 启动排序和分发线程
        self.dispatcher_thread = threading.Thread(target=self._dispatch_tasks, daemon=True)
        self.dispatcher_thread.start()

    def _select_account(self, shard_id) -> (dict, int):
        gen = self.account_generators[shard_id]
        return next(gen)

    def submit_signing_request(self, index, sub_index, tx, shard_id):
        """提交签名请求"""
        future = Future()
        with self.queue_lock:
            heapq.heappush(self.priority_queue, (index, sub_index, tx, shard_id, future))
            self.condition.notify()
        return future

    def _dispatch_tasks(self):
        """分发任务到工作线程"""
        while self.is_running:
            with self.queue_lock:
                while not self.priority_queue and self.is_running:
                    self.condition.wait()

                if not self.is_running:
                    break

                # 按优先级获取任务
                _, sub_index, tx, shard_id, future = heapq.heappop(self.priority_queue)
                account, nonce = self._select_account(shard_id)

            # 提交到线程池执行
            self.signer_executor.submit(self._sign_transaction, sub_index, tx, shard_id, account, nonce, future)

    def _sign_transaction(self, sub_index, tx, shard_id, account, nonce, future):
        """执行签名操作"""
        try:
            sign_start_time = time.time()
            tx['nonce'] = nonce
            tx['addr'] = account['address']
            signed_tx = signing.sign_transaction(tx, account["pri_key"])
            sign_time = time.time() - sign_start_time
            future.set_result(signed_tx)
        except Exception as e:
            future.set_exception(e)

    def shutdown(self):
        """关闭签名服务"""
        self.is_running = False
        with self.condition:
            self.condition.notify_all()
        self.signer_executor.shutdown(wait=True)


class HmyTCCSimulator:
    def __init__(self):
        self.env = "local"
        # self.env = "dev"
        self.tx_num = 100000
        self.path = "../../harmony/.hmy/expr_accounts"
        self.interval = 0.01
        self.timeout = 30
        max_tx_workers = 100
        self.tx_executor = ThreadPoolExecutor(max_workers=max_tx_workers, thread_name_prefix="TxWorker")
        self.accounts = defaultdict(list)
        self.account_generators = {}
        self.nonce_lock = threading.Lock()
        self.wait_lock = threading.Lock()
        self.logger = self.setup_logger()
        self.logger.info("start")
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
        with open("tcc_abi.json", 'r') as fp:
            abi = json.load(fp)
            w3 = Web3()
            self.contract = w3.eth.contract(abi=abi)
        for key_file in os.listdir(self.path):
            addr = '0x' + key_file.split(".")[0]
            shard_id = self.get_shard(addr)
            with open(os.path.join(self.path, key_file), "r") as fp:
                pri_key = fp.readline()
                self.accounts[shard_id].append({
                    "key_file": key_file,
                    "address": addr,
                    "shard_id": shard_id,
                    "nonce": 0,
                    "pri_key": pri_key,
                })
        for shard_id in self.accounts.keys():
            self.account_generators[shard_id] = self.account_generator(shard_id)
        self.signing_service = SigningService(self.account_generators)
        self.txs = pd.read_csv("simulate_data.csv")
        # self.txs = pd.read_csv("conflict_test_data.csv")
        self.results = []
        self.result_cnt = 0
        self.sub_txs = []
        self.result_lock = threading.Lock()
        self.target_path = "./"
        self.monitor = visual.ThreadMonitor()


    def init(self):
        self.monitor.start_monitoring()
        visualizer = visual.ConsoleVisualizer(self.monitor)
        # 启动可视化界面（在单独线程中）
        viz_thread = threading.Thread(target=visualizer.run, daemon=True)
        viz_thread.start()

    def setup_logger(self, log_file='app.log', console_level=logging.INFO, file_level=logging.INFO):
        """
        设置同时输出到控制台和文件的logger

        Args:
            name: logger名称
            log_file: 日志文件路径
            console_level: 控制台日志级别
            file_level: 文件日志级别

        Returns:
            配置好的logger实例
        """
        # 创建logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)  # 设置最低级别

        # 清除已有的handler，避免重复添加
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # 创建formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # 控制台handler
        # console_handler = logging.StreamHandler()
        # console_handler.setLevel(console_level)
        # console_handler.setFormatter(formatter)

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
        # logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger

    def account_generator(self, shard_id) -> (dict, int):
        i = 0
        accounts = self.accounts[shard_id]
        while True:
            curr_nonce = accounts[i]["nonce"]
            accounts[i]['nonce'] += 1
            yield accounts[i], curr_nonce
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

    def encode_abi(self, fn_name, args):
        data = self.contract.encodeABI(fn_name, kwargs=args)
        return data

    def get_endpoint(self, shard_id) -> str:
        if self.env == "local":
            return f"http://127.0.0.1:{9500 + 40 * shard_id}"
        if self.env == "dev":
            return f"http://{self.dev_ips[shard_id]}:{9500 + 40 * shard_id}"
        return "http://127.0.0.1:9500"

    def execute_cross_tx(self, index, index_shard, tx_tuple):
        shard_id = tx_tuple.shard_id
        to_addr = self.contracts[shard_id]
        json_args = json.loads(tx_tuple.tx_args.replace("'", '"'))
        sub_txs = []
        state_map = {}
        last_shard = -1
        states = []
        cnt = 0
        start_block = 0
        status = "running"
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
        thread_id = self.monitor.register_thread(f"cross_{index_shard}", len(sub_txs))
        self.monitor.update_thread_status(thread_id, "running", 0)
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
                    future = self.signing_service.submit_signing_request(index, sub_tx['index'], tx, shard_id)
                    signed_tx = future.result()
                    nonce = tx["nonce"]
                    if start_block == 0:
                        tx_start_time = time.time()
                    self.monitor.update_thread_status(thread_id, "waiting", sub_tx["index"])
                    tx_info = transaction.send_and_confirm_raw_transaction(
                        signed_tx.rawTransaction.hex(), local_net, self.timeout)
                    receipt = transaction.get_transaction_receipt(tx_info['hash'], local_net, timeout=self.timeout)
                    self.monitor.update_thread_status(thread_id, "running", sub_tx["index"])
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
                future = self.signing_service.submit_signing_request(index, len(sub_txs), unlock_tx, shard_id)
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
            self.monitor.update_thread_status(index, status, len(sub_txs))
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
        pd.DataFrame(self.results).to_csv(os.path.join(self.target_path, "result", f"simulate_tcc_results_{self.result_cnt}.csv"))

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

    def test(self):
        try:
            cnt = 0
            shard_cnts = [0] * len(self.contracts)
            futures = []
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
                time.sleep(self.interval)
            for index, future in enumerate(futures):
                self.logger.info(f"begin to wait for thread {index}")
                future.result()
            for shard_id, shard_cnt in enumerate(shard_cnts):
                self.logger.info(f"shard {shard_id}:{shard_cnt}/{cnt}")
        finally:
            self.save_results()
            sub_txs = []
            for tx in self.sub_txs:
                sub_txs.extend(tx)
            pd.DataFrame(sub_txs).to_csv(os.path.join(self.target_path, "sub_txs.csv"))


simulator = HmyTCCSimulator()
simulator.init()
simulator.test()
