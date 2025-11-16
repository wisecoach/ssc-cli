import json
import os
import time

from web3 import Web3
import web3.types as w3types
import pandas as pd

# 连接本地 Erigon 节点（默认 RPC 端口 8545）
erigon_url = "http://localhost:8545"
w3 = Web3(Web3.HTTPProvider(erigon_url, request_kwargs={"timeout": 300}))


class StateDiffFetcher:
    def __init__(self, web3_instance, first, last, chunk_size=100, data_dir="data"):
        self.w3 = web3_instance
        self.progress_file = "progress.txt"
        self.current_block = first
        self.current_tx_index = 0
        self.completed = False
        self.chunk_size = chunk_size
        self.first_block = first
        self.last_block = last
        self.data_dir = data_dir
        self.last_time = time.time()
        self.load_progress()
        if self.chunk_size != chunk_size:
            print(f"Chunk size changed from {self.chunk_size} to {chunk_size}.")
            self.chunk_size = chunk_size
        if self.first_block != first:
            print(f"First block changed from {self.first_block} to {first}.")
            self.first_block = first
        if self.last_block != last:
            print(f"Last block changed from {self.last_block} to {last}.")
            self.last_block = last
        if self.data_dir != data_dir:
            print(f"Data directory changed from {self.data_dir} to {data_dir}.")
            self.data_dir = data_dir
        self.balance_list = []
        self.storage_list = []

    def save_progress(self):
        # 保存进度到文件
        with open("progress.txt", "w") as f:
            progress = {
                "first_block": self.first_block,
                "last_block": self.last_block,
                "data_dir": self.data_dir,
                "chunk_size": self.chunk_size,
                "current_block": self.current_block,
                "current_tx_index": self.current_tx_index,
                "completed": self.completed
            }
            json.dump(progress, f)

    def load_progress(self):
        # 从文件加载进度
        if os.path.exists("progress.txt"):
            with open("progress.txt", "r") as f:
                progress = json.load(f)
                self.first_block = progress.get("first_block", self.first_block)
                self.last_block = progress.get("last_block", self.last_block)
                self.data_dir = progress.get("data_dir", self.data_dir)
                self.chunk_size = progress.get("chunk_size", self.chunk_size)
                self.current_block = progress.get("current_block", self.first_block)
                self.load_from_data()
                self.completed = progress.get("completed", False)

    def load_from_data(self):
        if os.path.exists(self.current_chunk_file("balance")):
            df = pd.read_csv(self.current_chunk_file("balance"))
            self.current_tx_index = int(df.iloc[-1]['tx_index'])
            self.current_block = int(df.iloc[-1]['block_number'])

    def save_failed_tx(self, tx_hash, block_number, tx_index):
        df = pd.DataFrame({
            "block_number": [block_number],
            "tx_hash": [tx_hash],
            "tx_index": [tx_index]
        })
        failed_file = os.path.join(self.data_dir, "failed_transactions.csv")
        if os.path.exists(failed_file):
            df.to_csv(failed_file, mode='a', header=False, index=False)
        else:
            df.to_csv(failed_file, index=False)

    def trace_replay_transaction(self, tx_hash):
        return self.w3.provider.make_request(
            w3types.RPCEndpoint("trace_replayTransaction"),
            [tx_hash, ["trace", "stateDiff"]]
        )

    def current_chunk_range(self):
        if self.current_block >= self.last_block:
            return self.last_block, self.last_block
        from_block = self.current_block // self.chunk_size * self.chunk_size
        to_block = from_block + self.chunk_size - 1
        if from_block < self.first_block:
            from_block = self.first_block
        if to_block > self.last_block:
            to_block = self.last_block
        return from_block, to_block

    def current_chunk_file(self, data_type):
        from_block, to_block = self.current_chunk_range()
        return os.path.join(self.data_dir, f"{data_type}_{from_block}_{to_block}.csv")

    def save_data(self):
        balance_file = self.current_chunk_file("balance")
        if os.path.exists(balance_file):
            df = pd.DataFrame(self.balance_list)
            df.to_csv(balance_file, mode='a', header=False, index=False)
        else:
            df = pd.DataFrame(self.balance_list)
            df.to_csv(balance_file, index=False)
        self.balance_list = []
        storage_file = self.current_chunk_file("storage")
        if os.path.exists(storage_file):
            df = pd.DataFrame(self.storage_list)
            df.to_csv(storage_file, mode='a', header=False, index=False)
        else:
            df = pd.DataFrame(self.storage_list)
            df.to_csv(storage_file, index=False)
        self.storage_list = []

    def handle_state_diff(self, state_diff, tx_hash, timestamp):
        if "ssc_result" not in state_diff:
            print(f"Error state_diff no ssc_result: {state_diff}")
            return
        if "stateDiff" not in state_diff["ssc_result"]:
            print(f"Error state_dff['ssc_result' no stateDiff]: {state_diff}")
            return
        for addr in state_diff["ssc_result"]["stateDiff"]:
            diff = state_diff["ssc_result"]["stateDiff"][addr]
            if "balance" in diff:
                if isinstance(diff["balance"], dict):
                    if "*" in diff["balance"]:
                        balance_diff = diff["balance"].get("*", {})
                        if "from" in balance_diff and "to" in balance_diff:
                            balance_data = {
                                "block_number": self.current_block,
                                "tx_hash": tx_hash,
                                "tx_index": self.current_tx_index,
                                "timestamp": timestamp,
                                "address": addr,
                                "from": balance_diff["from"],
                                "to": balance_diff["to"],
                            }
                            self.balance_list.append(balance_data)
                    elif "+" in diff["balance"]:
                        init_value = diff["balance"]["+"]
                        balance_data = {
                            "block_number": self.current_block,
                            "tx_hash": tx_hash,
                            "tx_index": self.current_tx_index,
                            "timestamp": timestamp,
                            "address": addr,
                            "from": 0,
                            "to": init_value,
                        }
                        self.balance_list.append(balance_data)
                    else:
                        print(f"diff['balance'] no ['*'] or ['+'], there is no balance update: {diff['balance']}")
                if isinstance(diff["storage"], dict):
                    for state_addr, state in diff["storage"].items():
                        if not("*" in state and "from" in state["*"] and "to" in state["*"]):
                            print(f"Error: {state_diff['ssc_result']['stateDiff'][addr]['storage']}")
                            continue
                        storage_data = {
                            "block_number": self.current_block,
                            "tx_hash": tx_hash,
                            "tx_index": self.current_tx_index,
                            "timestamp": timestamp,
                            "address": addr,
                            "state_addr": state_addr,
                            "from": state["*"]["from"],
                            "to": state["*"]["to"],
                        }
                        self.storage_list.append(storage_data)
        self.save_data()

    def fetch_state_diff_in_block(self, block_number):
        print(f"Fetching state diff for block {block_number}...")
        block = self.w3.eth.get_block(block_number, full_transactions=True)
        for tx in block.transactions:
            if block_number < self.current_block:
                print(f"Block {block_number} is already processed.")
                break
            if block_number == self.current_block and tx.transactionIndex < self.current_tx_index:
                print(f"Transaction {block_number}:{tx.transactionIndex} is already processed.")
                continue
            if hasattr(tx, "hash") and hasattr(tx, "transactionIndex"):
                tx_hash = tx.hash.to_0x_hex()
                self.current_tx_index = tx.transactionIndex
                print(f"Fetching state diff for transaction in block:{block_number}:{tx.transactionIndex}, last tx cost time: {time.time() - self.last_time:.2f} seconds")
                self.last_time = time.time()
                try:
                    state_diff = self.trace_replay_transaction(tx_hash)
                    self.handle_state_diff(state_diff, tx_hash, block.timestamp)
                    self.save_progress()
                except Exception as e:
                    print(f"Failed to fetch state diff for transaction in block {block_number}:{tx.transactionIndex}, err={e}")
                    self.save_failed_tx(tx_hash, block_number, tx.transactionIndex)

    def fetch(self):
        from_block, to_block = self.current_chunk_range()
        while from_block < to_block:
            from_block, to_block = self.current_chunk_range()
            start_block = self.current_block
            for block_num in range(start_block, to_block + 1):
                self.fetch_state_diff_in_block(block_num)
                self.save_progress()
                self.current_block = block_num + 1
                self.current_tx_index = 0


fetcher = StateDiffFetcher(w3, 20000000, 21000000, chunk_size=1000, data_dir="data")
fetcher.fetch()

