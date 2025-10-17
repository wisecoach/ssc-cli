import json
from dateutil.parser import parse


def calc_tps_delay(file):
    pooled_tx = {}
    committed_tx = {}
    start_time = None
    end_time = None
    with open(file, 'r') as log_file:
        for line in log_file:
            try:
                log_entry = json.loads(line)
                message = log_entry.get('message', '')
                time_str = log_entry['time']
                time = parse(time_str)
                if 'Pooled new transaction' in message:
                    tx_hash = log_entry['txHash']
                    pooled_tx[tx_hash] = time

                elif 'commit transaction' in message:
                    tx_hash = log_entry['txHash']
                    if tx_hash in pooled_tx:
                        start = pooled_tx[tx_hash]
                        committed_tx[tx_hash] = time

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
    for key in set(pooled_tx.keys()) & set(committed_tx.keys()):
        latency_sum += (committed_tx[key] - pooled_tx[key]).total_seconds()
    print(f"throughput: {len(committed_tx) / (end_time - start_time).total_seconds()}")
    print(f"all_latency: {latency_sum / len(committed_tx)}")


calc_tps_delay("test.log")