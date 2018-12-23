import subprocess
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import os, shutil
from os import listdir, walk
from os.path import isfile, join
from hdfs import InsecureClient
import re

mypath = '/home/hduser1/PravinFiles/AtlasCapco/Master/'

hdfs_path = '/user/pravinag/python/source'

local_dest = '/home/hduser1/PravinFiles/AtlasCapco/Master/processed/'


client_hdfs = InsecureClient('http://localhost:50070')

def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, shell=True)
    proc.communicate()
    return proc.returncode

cmd = ['hadoop', 'fs', '-test', '-e', hdfs_path]
code = run_cmd(cmd)
checkCMD = ['echo $?']

check = run_cmd(checkCMD)

if check == 1:

    print ('File not exist')

    cmd1 = ['hdfs', 'dfs', '-mkdir', hdfs_path]

    run_cmd(cmd1)

cmd = ['hadoop', 'fs', '-test', '-s', hdfs_path]
code = run_cmd(cmd)
checkCMD = ['echo $?']

check = run_cmd(checkCMD)

if check == 1:

    for f in listdir(mypath):

        if isfile(join(mypath, f)):
            files = join(mypath, f)

            tree = ET.parse(files)
            root = tree.getroot()

            get_range = lambda col: range(len(col))
            l = [{r[i].tag: r[i].text for i in get_range(r)} for r in root]

            df = pd.DataFrame.from_dict(l)

            df.columns = [re.sub(r'{.*}', '', col) for col in df.columns]

            with client_hdfs.write(hdfs_path + '/' + f + '.csv', encoding='utf-8') as writer:
                df.loc[:, df.columns != 'element'].dropna(how='all') \
                    .to_csv(writer, index=False)


else:

    print('File exist')

    for f in listdir(mypath):

        if isfile(join(mypath, f)):
            files = join(mypath, f)

            tree = ET.parse(files)
            root = tree.getroot()

            get_range = lambda col: range(len(col))
            l = [{r[i].tag: r[i].text for i in get_range(r)} for r in root]

            df = pd.DataFrame.from_dict(l)

            df.columns = [re.sub(r'{.*}', '', col) for col in df.columns]

            code2 = ['hadoop','fs','-test', '-e', hdfs_path + '/' + f + '.csv']

            check_file = run_cmd(code2)

            if check_file == 1:

                 with client_hdfs.write(hdfs_path + '/' + f + '.csv', encoding='utf-8') as writer:
                      df.loc[:, df.columns != 'element'].dropna(how='all') \
                         .to_csv(writer, index=False)
            else:
                print ('CSV files already exist')



if not os.path.exists(local_dest):

    os.makedirs(local_dest)



files = os.listdir(mypath)

files.sort()

for f in files:

    source_path = mypath+f
    destination_path = local_dest+f

    exists = os.path.isfile(source_path)

    if exists:

      shutil.move(source_path,destination_path)

    else:
        print ('Files already moved')




