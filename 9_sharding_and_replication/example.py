#!/usr/bin/env python3

from requests import get
from requests import post

import json


def queries():
    response = get('http://127.0.0.1:8080/queries')
    return json.loads(response.text)['success']


def select(query, verbose=True):
    response = get('http://127.0.0.1:8080/select', params={'query': query})
    if verbose:
        print(response.text)


def update(json_object, verbose=True):
    response = post('http://127.0.0.1:8080/update', data=json.dumps(json_object))
    if verbose:
        print(response.text)


from os import chdir
from os.path import dirname
from time import time

chdir(dirname(dirname(__file__)))
chdir('./esci-data/shopping_queries_dataset')

import pandas as pd

df_products = pd.read_parquet('shopping_queries_dataset_products.parquet')
df_products = df_products['us' == df_products.product_locale]
post_size = len(df_products) // 100

print('2')

for _ in range(2):
    buffer, start = [], time()

    for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
        buffer.append({'product_id': product_id, 'product_title': product_title})
        if post_size <= len(buffer):
            update(buffer, verbose=False)
            buffer = []

    if 0 < len(buffer):
        update(buffer, verbose=False)
    print('Elapsed Time: {0} (s)'.format(time() - start))

print('4')

start = time()

for query in queries():
    select(query, verbose=False)

print('Elapsed Time: {0} (s)'.format(time() - start))
