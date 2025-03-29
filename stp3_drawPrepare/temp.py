import pandas as pd
import os
from collections import Counter
import glob

input_dir = '../../stp2_simp'
output_dir = '../../stp3_drawPrepare'

# Prepare for folder
os.makedirs(output_dir, exist_ok=True)

for file_path in glob.glob(os.path.join(input_dir, '*.csv')):
    # Read CSV
    df = pd.read_csv(file_path)

    new_rows = []

    for _, row in df.iterrows():
        # Get basic info
        pos = row['POS']
        ref = row['REF']

        # Count
        values = row[2:].dropna().tolist()
        counter = Counter(values)

        # Sort
        sorted_items = sorted(
            counter.items(),
            key=lambda x: (-x[1], x[0])
        )

        # Move REF to top
        ref_index = next(
            (i for i, (k, _) in enumerate(sorted_items) if k == ref), None)
        if ref_index is not None:
            ref_item = sorted_items.pop(ref_index)
            sorted_items.insert(0, ref_item)

        # Gen new col data
        new_row = {'POS': pos, 'REF': ref}
        for idx, (val, cnt) in enumerate(sorted_items):
            new_row[f'value_{idx+1}'] = val
            new_row[f'count_{idx+1}'] = cnt

        new_rows.append(new_row)

    # New a df
    df_new = pd.DataFrame(new_rows)

    # Rename POS
    df_new['POS'] = [f'M{i+1}' for i in range(len(df_new))]

    # Keep POS, REF, value_1, count_1, value_2, count_2...
    cols = ['POS', 'REF'] + [
        f'{prefix}_{i}'
        for i in range(1, 1 + len(df_new.columns)//2)
        for prefix in ['value', 'count']
    ][:len(df_new.columns)-2]
    del cols[1]

    df_new.drop(columns=["REF"], inplace=True)

    # Save
    output_path = os.path.join(output_dir, os.path.basename(file_path))
    df_new[cols].to_csv(output_path, index=False)

print("All things done.")
