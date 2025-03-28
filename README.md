# About
This is a kit to visualize the SNP.  
**Could only support VCF file at source!**
**Some codes need to be rewrite**, but they are able to use, just they are unclean& beauty for me.

# How to use
## stp0, prepare for your data& this kit
To prepare the datas for you project, you must be followd by:
+ Make sure you are in a special, empty folder for this work.
+ Creat a folder named "vcf", move all .vcf file to there.
+ Run bash/cmd/powershell, then run ```git clone https://github.com/DoubleBobCat/SNPVisualize.git```, rename cloned folder to "scripts". Make sure you have already installed the **"Git"** before do that. Or you can download this repositories and handfully unzip codes, move them to a new child folder "scripts".

To prepare for this respositories, keep your bash/cmd/powershell windows, then run ```conda insall requirements.txt```.

## stp1, convert VCF to CSV
In this step, you can convert vcf files to csv files as data, json files as information.

Just makesure your bash/cmd/powershell work folder is the script folder, then run ```python process_vcfSpilit.py```.

After running, a new folder "vcf_split" will be create in you root folder for this work, and there will have two types of file. The "csv" file is stored the SNP data, the "json" file is stored the vcf run info.