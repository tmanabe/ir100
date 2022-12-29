#!/usr/bin/env python3

from os import chdir
from os.path import dirname

chdir(dirname(dirname(__file__)))

# 0

from os import system

assert 0 == system('brew install git-lfs')
assert 0 == system('git lfs install')

# 1

from os.path import exists

if not exists('esci-data'):
    system('git clone git@github.com:amazon-science/esci-data.git')

# 2

assert 0 == system('pip3 install pandas')

# 3

chdir('./esci-data/shopping_queries_dataset')

import pandas as pd

df_products = pd.read_parquet('shopping_queries_dataset_products.parquet')

# 4

for _ in range(10):
    print(df_products.sample())
