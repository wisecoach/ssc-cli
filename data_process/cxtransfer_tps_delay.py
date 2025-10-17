import json
import os
import shutil
from dateutil.parser import parse

root_path = "/mnt/E/gowork/src/github.com/harmony-one/harmony"


def get_log():
    logs_path = os.path.join(root_path, "tmp_log")
    for root, dirs, files in os.walk(logs_path):
        for file in files:
            if file.endswith(".log"):
                if file.__contains__("9000"):
                    return os.path.join(root, file)


def resolve_log(log_file):
    shutil.copy(log_file, "data/temp.log")
    os.chdir("data")
    os.system("cat temp.log | grep 'ssc' > temp_ssc.log")
    with open("temp_ssc.log", 'r') as fp:
        for line in fp:
            log_entry = json.loads(line)
            if 'txHash' in log_entry:
                txHash = log_entry['txHash']
                os.system(f"cat temp_ssc.log | grep {txHash} > ssc_tx.log")
                break


def calc_tps_delay(file):
    all_tx_sum = 0
    pooled_tx = {}
    committed_tx = {}
    simulate_start_time = {}
    simulation_commit_time = {}
    inner_tx = {}
    cross_tx = {}
    start_time = None
    end_time = None
    contract_num = 2
    with open(file, 'r') as log_file:
        for line in log_file:
            try:
                log_entry = json.loads(line)
                message = log_entry.get('message', '')
                time_str = log_entry['time']
                time = parse(time_str)

                # 确保合约部署之后才开始计算
                if 'Submitted contract creation' in message:
                    contract_num -= 1
                if contract_num > 0:
                    continue

                # 交易进入池子
                if 'Pooled new transaction' in message:
                    tx_hash = log_entry['txHash']
                    pooled_tx[tx_hash] = time

                # 开始跨分片交易模拟阶段
                elif 'start simulate cx transaction, start' in message:
                    tx_hash = log_entry['txHash']
                    simulate_start_time[tx_hash] = time

                # 跨分片交易模拟提交
                elif "simulation commit" in message:
                    tx_hash = log_entry['txHash']
                    simulation_commit_time[tx_hash] = time

                # 跨分片交易提交证明
                elif 'commit with proof' in message:
                    tx_hash = log_entry['txHash']
                    if tx_hash in pooled_tx:
                        start = pooled_tx[tx_hash]
                        committed_tx[tx_hash] = time

                        cross_tx[tx_hash] = {
                            "start": start,
                            "simulate_start": simulate_start_time[tx_hash],
                            "simulation_commit": simulation_commit_time[tx_hash],
                            "end": time
                        }

                        # 更新总体时间范围以计算吞吐量
                        if start_time is None or start < start_time:
                            start_time = start
                        if end_time is None or time > end_time:
                            end_time = time

                # 内部交易提交证明
                elif 'commit transaction' in message:
                    # 计算所有交易数量，包括跨分片交易执行所需的模拟结果提交以及最终提交
                    all_tx_sum += 1
                    if 'crossShard' in log_entry and log_entry['crossShard']:
                        continue
                    tx_hash = log_entry['txHash']
                    if tx_hash in pooled_tx:
                        start = pooled_tx[tx_hash]
                        committed_tx[tx_hash] = time

                        inner_tx[tx_hash] = {
                            "start": start,
                            "end": time
                        }

                        # 更新总体时间范围以计算吞吐量
                        if start_time is None or start < start_time:
                            start_time = start
                        if end_time is None or time > end_time:
                            end_time = time

            except json.JSONDecodeError:
                # 忽略无法解析的行
                continue
    # 计算平均时延
    latency_sum = 0
    simulation_duration_sum = 0
    inner_latency_sum = 0
    cross_latency_sum = 0
    sum_cnt = len(inner_tx) + len(cross_tx)
    for tx_hash, tx in inner_tx.items():
        latency_sum += (tx['end'] - tx['start']).total_seconds()
        inner_latency_sum += (tx['end'] - tx['start']).total_seconds()
    for tx_hash, tx in cross_tx.items():
        latency_sum += (tx['end'] - tx['start']).total_seconds()
        simulation_duration_sum += (tx['simulation_commit'] - tx['simulate_start']).total_seconds()
        cross_latency_sum += (tx['end'] - tx['start']).total_seconds()

    print(f"test_time: {(end_time-start_time).total_seconds()}s, start_time: {start_time}, end_time: {end_time}")
    print(f"throughput: {len(committed_tx) / (end_time - start_time).total_seconds()}")
    print(f"tx_cnt: {len(inner_tx) + len(cross_tx)}")
    print(f"all tx sum: {all_tx_sum}")

    if len(inner_tx) > 0:
        print(f"inner_tx_cnt: {len(inner_tx)}")
        print(f"inner_throughput: {len(inner_tx) / (end_time - start_time).total_seconds()}")
        print(f"inner_latency: {inner_latency_sum / len(inner_tx)}")

    if len(cross_tx) > 0:
        print(f"cross_tx_cnt: {len(cross_tx)}")
        print(f"cross_throughput: {len(cross_tx) / (end_time - start_time).total_seconds()}")
        print(f"cross_latency: {cross_latency_sum / len(cross_tx)}")
        print(f"simulate_duration: {simulation_duration_sum / len(committed_tx)}")

    print(f"all_latency: {latency_sum / sum_cnt}")


process_local_log = False

if process_local_log:
    for log_file in os.listdir("data/base"):
        print(f"process base log file: {log_file}")
        calc_tps_delay(os.path.join("data/base", log_file))
else:
    log_file = get_log()
    resolve_log(log_file)
    print(f"process log file: {log_file}")
    calc_tps_delay(log_file)