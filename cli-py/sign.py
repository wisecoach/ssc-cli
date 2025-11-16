import heapq
import os

import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future

from pyhmy import signing
from utils import get_shard

from eth_keys import keys

class SigningService:
    def __init__(self, path, max_signer_workers=10):
        self.signer_executor = ThreadPoolExecutor(max_workers=max_signer_workers, thread_name_prefix="Signer")
        self.priority_queue = []
        self.queue_lock = threading.Lock()
        self.condition = threading.Condition(self.queue_lock)
        self.is_running = True
        self.path = path
        self.accounts = defaultdict(list)
        self.account_generators = self.init_account_generators()
        # 启动排序和分发线程
        self.dispatcher_thread = threading.Thread(target=self._dispatch_tasks, daemon=True)
        self.dispatcher_thread.start()

    def init_account_generators(self):
        account_generators = {}
        shard_ids = {}
        for key_file in os.listdir(self.path):
            addr = '0x' + key_file.split(".")[0]
            shard_id = get_shard(addr)
            with open(os.path.join(self.path, key_file), "r") as fp:
                pri_key = fp.readline()
                self.accounts[shard_id].append({
                    "key_file": key_file,
                    "address": addr,
                    "shard_id": shard_id,
                    "nonce": 0,
                    "pri_key": pri_key,
                })
                shard_ids[shard_id] = shard_id
        for shard_id in shard_ids.keys():
            account_generators[shard_id] = self.account_generator(shard_id)
        return account_generators

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

    def _select_account(self, shard_id) -> (dict, int):
        gen = self.account_generators[shard_id]
        return next(gen)

    def submit_signing_request(self, index, tx, shard_id):
        """提交签名请求"""
        future = Future()
        with self.queue_lock:
            heapq.heappush(self.priority_queue, (index, tx, shard_id, future))
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
                _, tx, shard_id, future = heapq.heappop(self.priority_queue)
                account, nonce = self._select_account(shard_id)

            # 提交到线程池执行
            self.signer_executor.submit(self._sign_transaction, tx, account, nonce, future)

    def _sign_transaction(self, tx, account, nonce, future):
        """执行签名操作"""
        try:
            sign_start_time = time.time()
            tx['nonce'] = nonce
            tx['addr'] = account['address']
            signed_tx = signing.sign_transaction(tx, account["pri_key"])
            sign_time = time.time() - sign_start_time
            print(f"sign transaction: addr={account['address']}, nonce={nonce}, time_cost={sign_time:.6f}s")
            future.set_result(signed_tx)
        except Exception as e:
            future.set_exception(e)

    def shutdown(self):
        """关闭签名服务"""
        self.is_running = False
        with self.condition:
            self.condition.notify_all()
        self.signer_executor.shutdown(wait=True)