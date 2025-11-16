import json
import os
from collections import defaultdict

import pandas as pd
import time
import numpy as np
from dateutil import parser


class DataProcessor:

    log_events = [
        "add",
        "commit",
        "rollback",
        "timeout",
        "simulation_failed",
        "unknown"
    ]

    tx_status = [
        "unsent",
        "unfinished",
        "commit",
        "rollback",
        "timeout",
        "simulation_failed",
        "error",
    ]

    def __init__(self, tx_num=100000, shard_num=4, log_path="../../ssc-harmony/tmp_log", output_dir="./output", simulate_data="../cli-py/tx_logs.csv"):
        self.tx_num = tx_num
        self.log_path = log_path
        self.output_dir = output_dir
        self.shard_num = shard_num
        self.simulate_data = simulate_data
        self.ssc_logs = {}
        self.zero_logs = {}
        self.hash2index = {}
        self.txs = []
        self.init()

    def init(self):
        exam_log_path = os.listdir(self.log_path)[-1]
        for log in os.listdir(os.path.join(self.log_path, exam_log_path)):
            for i in range(self.shard_num):
                if self.get_port(i) in log and "ssc" in log:
                    self.ssc_logs[i] = os.path.join(self.log_path, exam_log_path, log)
                if self.get_port(i) in log and "zero" in log:
                    self.zero_logs[i] = os.path.join(self.log_path, exam_log_path, log)
        df = pd.read_csv(self.simulate_data)
        cnt = 0
        for index, row in df.iterrows():
            if cnt >= self.tx_num:
                break
            self.hash2index[row.txHash] = index
            self.txs.append({"txHash": row.txHash, "index": index, "shard": row.shard, "shard_index": row.shard_index,
                             "send": {"txHash": row.txHash, "time": time.localtime(row.send_time)}})
            cnt += 1


    def get_tx(self, tx_hash) -> dict:
        if tx_hash not in self.hash2index:
            return {}
        return self.txs[self.hash2index[tx_hash]]

    def get_port(self, shard_id) -> str:
        return f"{9000+shard_id*40}"

    def process_base(self):
        processed_txs = []
        status_cnt = defaultdict(int)
        for i in range(self.shard_num):
            with open(self.ssc_logs[i], 'r') as fp:
                for line in fp:
                    log_obj = json.loads(line)
                    event_type, event_data = self.get_log_event(log_obj)
                    match event_type:
                        case "add":
                            self.get_tx(event_data['txHash'])['add'] = event_data
                        case "commit":
                            self.get_tx(event_data['txHash'])['commit'] = event_data
                        case "rollback":
                            self.get_tx(event_data['txHash'])['rollback'] = event_data
                        case "simulate":
                            self.get_tx(event_data['txHash'])['simulate'] = event_data
                        case "timeout":
                            self.get_tx(event_data['txHash'])['timeout'] = event_data
                        case "simulation_failed":
                            self.get_tx(event_data['txHash'])['simulation_failed'] = event_data
                        case "unknown":
                            continue
        for tx in self.txs:
            _, processed_tx = self.process_tx(tx)
            processed_txs.append(processed_tx)
            status_cnt[processed_tx['status']] += 1
        df = pd.DataFrame(processed_txs)
        df.to_csv(os.path.join(self.output_dir, "processed_txs_15.csv"), index=False)
        print(status_cnt)


    def process_tx(self, tx):
         if "commit" in tx:
             if "add" in tx and "simulate" in tx:
                 processed_tx = {
                     "index": tx["index"],
                     "shard": tx['shard'],
                     "shard_index": tx["shard_index"],
                     "txHash": tx["txHash"],
                     "status": "commit",
                     "time": (tx["commit"]["time"] - tx["add"]["time"]).total_seconds(),
                     "start_time": tx["add"]["time"],
                     "end_time": tx["commit"]["time"],
                     "simulationNum": tx["simulate"]["simulationNum"],
                 }
                 return "commit", processed_tx
         if "timeout" in tx:
             if "add" in tx and "simulate" in tx:
                 processed_tx = {
                     "index": tx["index"],
                     "shard": tx['shard'],
                     "shard_index": tx["shard_index"],
                     "txHash": tx["txHash"],
                     "status": "timeout",
                     "time": (tx["timeout"]["time"] - tx["add"]["time"]).total_seconds(),
                     "start_time": tx["add"]["time"],
                     "end_time": tx["timeout"]["time"],
                     "simulationNum": tx["simulate"]["simulationNum"],
                 }
                 return "timeout", processed_tx
         if "rollback" in tx:
             if "add" in tx and "simulate" in tx:
                 processed_tx = {
                     "index": tx["index"],
                     "shard": tx['shard'],
                     "shard_index": tx["shard_index"],
                     "txHash": tx["txHash"],
                     "status": "rollback",
                     "time": (tx["rollback"]["time"] - tx["add"]["time"]).total_seconds(),
                     "start_time": tx["add"]["time"],
                     "end_time": tx["rollback"]["time"],
                     "simulationNum": tx["simulate"]["simulationNum"],
                 }
                 return "rollback", processed_tx
         if "simulation_failed" in tx:
             if "add" in tx:
                 processed_tx = {
                     "index": tx["index"],
                     "shard": tx['shard'],
                     "shard_index": tx["shard_index"],
                     "txHash": tx["txHash"],
                     "status": "simulation_failed",
                     "time": (tx["simulation_failed"]["time"] - tx["add"]["time"]).total_seconds(),
                     "start_time": tx["add"]["time"],
                     "end_time": tx["simulation_failed"]["time"],
                 }
                 return "simulation_failed", processed_tx
         if "add" in tx and "simulate" in tx:
             return "unfinished", {
                 "index": tx["index"],
                 "shard": tx['shard'],
                 "shard_index": tx["shard_index"],
                 "txHash": tx["txHash"],
                 "status": "unfinished",
                 "start_time": tx["add"]["time"],
                 "simulationNum": tx["simulate"]["simulationNum"],
             }
         if "add" in tx:
             return "unsimulated", {
                 "index": tx["index"],
                 "shard": tx['shard'],
                 "shard_index": tx["shard_index"],
                 "txHash": tx["txHash"],
                 "status": "unsimulated",
                 "start_time": tx["add"]["time"],
             }
         return "unsent", {
             "index": tx["index"],
             "shard": tx['shard'],
             "shard_index": tx["shard_index"],
             "txHash": tx["txHash"],
             "status": "unsent",
         }


    def get_log_event(self, log_obj) -> (str, dict):
        if "add a cross shard Tx" in log_obj['message']:
            if log_obj['isPreCompiled'] is False:
                return "add", {"time": parser.isoparse(log_obj["time"]), "txHash": log_obj["txHash"]}
        if "commit with proof" in log_obj['message']:
            if "[true," in log_obj['message']:
                return "commit", {"time": parser.isoparse(log_obj["time"]), "txHash": log_obj["txHash"]}
        if "rollback with proof, origin" in log_obj['message']:
            if "[true," in log_obj['message']:
                if "ReasonCxtTimeoutForSp1" in log_obj['message']:
                    return "timeout", {"time": parser.isoparse(log_obj["time"]), "txHash": log_obj["txHash"]}
                else:
                    return "rollback", {"time": parser.isoparse(log_obj["time"]), "txHash": log_obj["txHash"]}
        if "simulate request, start" in log_obj['message']:
            return "simulate", {"time": parser.isoparse(log_obj["time"]), "txHash": log_obj["txHash"], "simulationNum": log_obj["simulationNum"]}
        if "simulation" in log_obj['message'] and "failed, status=ExecutionFailed" in log_obj['message']:
            return "simulation_failed", {"time": parser.isoparse(log_obj["time"]), "txHash": log_obj["txHash"]}
        return "unknown", {"time": log_obj["time"]}


processor = DataProcessor()
processor.process_base()
