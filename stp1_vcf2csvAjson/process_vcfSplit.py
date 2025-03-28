import os
import vcf2csvAjson as v2caj

vcf_lib = '../vcf'
vcf_split_lib = '../vcf_split'

if not os.path.exists((vcf_lib)):
    print(f"E: No such dir {vcf_lib}")
    exit(1)
if not os.path.exists(vcf_split_lib):
    os.mkdir(vcf_split_lib)

for files in os.listdir(vcf_lib):
    if files.endswith('.vcf'):
        vcf_path = os.path.join(vcf_lib, files)
        vcf_json_path = os.path.join(vcf_split_lib, files.replace(".vcf",".json"))
        vcf_csv_path = os.path.join(vcf_split_lib, files.replace(".vcf", ".csv"))
        v2caj.vcf2jsonAcsv(vcf_path, vcf_json_path, vcf_csv_path)
        print(f"I: Processed {vcf_path}")
