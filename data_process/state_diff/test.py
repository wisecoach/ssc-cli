import json

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

with open("block_20000000-20010000_shard_prefix_2_unlock_normal_conflict_fail_delay_0/conflict_addr2cnt.json", "r") as f:
    conflict_map = json.load(f)
    data = []
    for key, value in conflict_map.items():
        data.append({
            "value": value,
            "key": key,
        })
    df = pd.DataFrame(data)
    df.sort_values("value", ascending=False, inplace=True)
    plt.plot(np.arange(len(conflict_map)), df['value'])
    plt.xscale('log')
    plt.yscale('log')
    plt.show()
    print(df.head())