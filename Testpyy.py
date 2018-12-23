import subprocess
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import os
from hdfs import InsecureClient
import re

tree = ET.parse("/home/hduser1/PravinFiles/AtlasCapco/Master/Avg_Master_Feed_Pressure.xml")
root = tree.getroot()

get_range = lambda col: range(len(col))
l = [{r[i].tag:r[i].text for i in get_range(r)} for r in root]

df = pd.DataFrame.from_dict(l)

df.columns = [re.sub(r'{.*}', '', col) for col in df.columns]

df.loc[:, df.columns != 'element'].dropna(how='all') \
    .to_csv("/home/hduser1/PravinFiles/AtlasCapco/Master/csv/Avg_Master_Feed_Pressure6.csv", index=False)