import pandas as pd
import os
from collections import Counter
import glob

input_dir = '../../csv_first'
output_dir = '../../second_csv'

# 确保输出目录存在
os.makedirs(output_dir, exist_ok=True)

for file_path in glob.glob(os.path.join(input_dir, '*.csv')):
    # 读取CSV文件
    df = pd.read_csv(file_path)

    new_rows = []

    for _, row in df.iterrows():
        # 提取基础信息
        pos = row['POS']
        ref = row['REF']

        # 统计后续列的值
        values = row[2:].dropna().tolist()  # 排除可能的NaN值
        counter = Counter(values)

        # 排序处理
        sorted_items = sorted(
            counter.items(),
            key=lambda x: (-x[1], x[0])  # 先按计数降序，再按键升序
        )

        # 检查并移动REF到首位
        ref_index = next(
            (i for i, (k, _) in enumerate(sorted_items) if k == ref), None)
        if ref_index is not None:
            ref_item = sorted_items.pop(ref_index)
            sorted_items.insert(0, ref_item)

        # 构建新行数据
        new_row = {'POS': pos, 'REF': ref}
        for idx, (val, cnt) in enumerate(sorted_items):
            new_row[f'value_{idx+1}'] = val
            new_row[f'count_{idx+1}'] = cnt

        new_rows.append(new_row)

    # 创建新DataFrame
    df_new = pd.DataFrame(new_rows)

    # 重命名POS列
    df_new['POS'] = [f'M{i+1}' for i in range(len(df_new))]

    # 保持列顺序：POS, REF, value_1, count_1, value_2, count_2...
    cols = ['POS', 'REF'] + [
        f'{prefix}_{i}'
        for i in range(1, 1 + len(df_new.columns)//2)
        for prefix in ['value', 'count']
    ][:len(df_new.columns)-2]
    del cols[1]

    df_new.drop(columns=["REF"], inplace=True)

    # 保存结果
    output_path = os.path.join(output_dir, os.path.basename(file_path))
    df_new[cols].to_csv(output_path, index=False)

print("所有文件处理完成！")
