import yaml

HEX_TO_BITS = {
    '0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7,
    '8': 8, '9': 9, 'A': 10, 'B': 11, 'C': 12, 'D': 13, 'E': 14, 'F': 15,
    'a': 10, 'b': 11, 'c': 12, 'd': 13, 'e': 14, 'f': 15
}

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


def resolve_contract_shard():
    with open('../data/contracts.yaml', 'r', encoding='utf-8') as file:
        data = yaml.safe_load(file)
        contracts = data['contracts']
        for (n, shard_num)  in enumerate([2, 4, 8, 16, 32]):
            shard_mapping = {}
            for contract in contracts:
                shard_id = get_shard(contract, n=n+1)
                if shard_id not in shard_mapping:
                    shard_mapping[shard_id] = contract
                if len(shard_mapping) == shard_num:
                    break
            data[f"contracts_for_{shard_num}_shards"] = shard_mapping
    with open('../data/new_contracts.yaml', 'w', encoding='utf-8') as file:
        yaml.dump(data, file, allow_unicode=True)


if __name__ == "__main__":
    resolve_contract_shard()