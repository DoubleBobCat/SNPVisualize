# FILE
import config
import main
# MODULE
import pandas as pd
from collections.abc import Iterable
import re


def pre_processing(df: pd.DataFrame, file_name: str, column_names: Iterable[str], all_count: int, use_Edge: bool = False) -> pd.DataFrame:
    if use_Edge:
        for col_name in column_names:
            df[col_name] = df[col_name].astype(str)
    else:
        for col_name in column_names:
            df[col_name] = df[col_name].astype(str)
    # Init base_ref
    with config.base_ref_lock:
        config.base_ref[file_name] = dict()
    for index, line in df.iterrows():
        # prepare base ref
        ref_list = []
        if ',' in line["REF"]:
            ref_list.extend(line["REF"].split(","))
        else:
            ref_list.append(line["REF"])
        if ',' in line["ALT"]:
            ref_list.extend(line["ALT"].split(","))
        else:
            ref_list.append(line["ALT"])
        with config.base_ref_lock:
            config.base_ref[file_name].update({
                index: ref_list
            })
        config.update_progress_queue.put((
            file_name,
            {
                "processed_count": all_count,
                "data": "\\"
            },
            False
        ))
    return df


def processing(df_shard: pd.DataFrame, file_name: str, shard_data: dict, column_names: list) -> pd.DataFrame:
    for row_idx in range(shard_data["row_begin"], shard_data["row_end"]+1):
        for col_idx in range(shard_data["col_begin"], shard_data["col_end"]+1):
            # Get GT data
            GT_data = df_shard.loc[row_idx, column_names[col_idx]].split(':')[
                0]
            # Get GT str data
            if (GT_data[0] == ".") and not (("/" in GT_data) or ("|" in GT_data)):
                GT_data_str = "-"
            else:
                GT_posIds = re.split(r'[/|]', GT_data)
                GT_posStr = []
                for GT_posId in GT_posIds:
                    if GT_posId != ".":
                        with config.base_ref_lock:
                            GT_posStr.append(
                                config.base_ref[file_name][row_idx][int(
                                    GT_posId)]
                            )
                    else:
                        GT_posStr.append("-")
                GT_posStr = list(set(GT_posStr))
                GT_data_str = GT_posStr[0] if len(GT_posStr) == 1 else '/'.join(
                    sorted(
                        GT_posStr,
                        key=lambda x: config.RUN_CONFIG["BASE_ORDER"][x]
                    )
                )
            # Set Cleaned str data
            df_shard.loc[row_idx, column_names[col_idx]] = GT_data_str
        config.update_progress_queue.put((
            file_name,
            int(shard_data["col_end"]-shard_data["col_begin"]+1),
            df_shard.loc[row_idx, column_names[col_idx]]
        ))
        if config.force_exit:
            exit(1)
    return df_shard


def post_processing(df: pd.DataFrame, column_names: Iterable[str], use_Edge: bool = False) -> pd.DataFrame:
    df.drop(
        columns=["#CHROM", "ID", "ALT",
                 "QUAL", "FILTER", "INFO", "FORMAT"],
        inplace=True
    )
    return df
