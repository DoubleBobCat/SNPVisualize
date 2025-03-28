from typing import Union
import re
import pandas as pd
import json


# vcf_meta_information_parsing
def vcf_meta_information_parsing(meta_information) -> Union[str, any]:
    # Basic Process
    meta_information = meta_information[2:]
    if '\n' in meta_information:
        meta_information = meta_information.replace('\n', '')
    elif '\t' in meta_information:
        meta_information = meta_information.replace('\r', '')
    # split Type & Data
    meta_information_type, meta_information_data = meta_information.split(
        '=', 1)
    # Filter the filedescript
    if '<' in meta_information_data:
        meta_information_data = meta_information_data.replace('<', '')
        meta_information_data = meta_information_data.replace('>', '')
        # Process Data in <> to dict:all_data
        all_data = dict()
        for each_data in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', meta_information_data):
            each_data_name, each_data_data = each_data.split('=', 1)
            all_data[each_data_name] = each_data_data
        return meta_information_type, all_data
    else:
        return meta_information_type, meta_information_data


# vcf2jsonAcsv
def vcf2jsonAcsv(file, json_file, csv_file):
    json_data = dict()
    with open(file, 'r') as f:
        f_lines = f.readlines()
        global data_frame_begin
        for line, iter in zip(f_lines, range(len(f_lines))):
            # vcf_meta_information
            if line.startswith('##'):
                MI_type, MI_data = vcf_meta_information_parsing(line)
                if MI_type in json_data.keys():
                    json_data[MI_type].append(MI_data)
                else:
                    json_data[MI_type] = [MI_data]
            # vcf_data_frame
            else:
                data_frame_begin = iter
                break
        del f_lines[0:data_frame_begin]
    name = f_lines[0].replace("\n", "").split('\t')
    del f_lines[0]
    # Json
    vcf_meta_information = json.dumps(json_data)
    with open(json_file, 'w+') as f:
        f.write(vcf_meta_information)
    # CSV
    vcf_data_frame_data = [line.replace(
        "\n", "").split('\t') for line in f_lines]
    vcf_data_frame = pd.DataFrame(
        data=vcf_data_frame_data, index=None, columns=name)
    vcf_data_frame.to_csv(csv_file, index=False)
