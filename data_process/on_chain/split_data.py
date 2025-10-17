import pandas as pd


def split_csv_with_pandas(input_file, output_prefix, chunk_size=10000):
    # 读取大文件，分块处理
    for i, chunk in enumerate(pd.read_csv(input_file, chunksize=chunk_size)):
        # 生成输出文件名
        output_file = f"{output_prefix}_{i+1}.csv"
        # 保存分块数据到新文件
        chunk.to_csv(output_file, index=False)
        print(f"已创建: {output_file}")


# 使用示例
split_csv_with_pandas('data/0to999999_BlockTransaction.csv', 'chunks/txs')
split_csv_with_pandas('data/0to999999_InternalTransaction.csv', 'chunks/itxs')
