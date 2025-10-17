import os

import pandas as pd

merge_df = pd.DataFrame()

for file in os.listdir("./"):
    if file.endswith(".csv"):
        df = pd.read_csv(file)
        # merge df into merge_df
        merge_df = pd.concat([merge_df, df], ignore_index=True)
        print("merge file:", file)
merge_df.to_csv("../merged_simulate_data.csv", index=False)

