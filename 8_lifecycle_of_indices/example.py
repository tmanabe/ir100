#!/usr/bin/env python3

from requests import get
from requests import post

import json


def update(json_object, verbose=True):
    response = post('http://127.0.0.1:8080/update', data=json.dumps(json_object))
    if verbose:
        print(response.text)


def select(word, verbose=True):
    response = get('http://127.0.0.1:8080/select', params={'query': word})
    if verbose:
        print(response.text)


def delete(product_id, verbose=True):
    response = get('http://127.0.0.1:8080/delete', params={'product_id': product_id})
    if verbose:
        print(response.text)


print('0')
update([
    {"product_id": "x", "product_title": "a b c"},
    {"product_id": "y", "product_title": "a b"},
    {"product_id": "z", "product_title": "a"},
])
select('b')

print('1')
update([
    {"product_id": "v", "product_title": "c c"},
    {"product_id": "w", "product_title": "c c c"},
])
select('c')

print('2')
delete('v')
select('c')

print('3')
update([
    {"product_id": "x", "product_title": "a b a b"},
    {"product_id": "y", "product_title": "c c c c"},
])
select('c')

# Omit 4.

from os import chdir
from os.path import dirname
from time import time

chdir(dirname(dirname(__file__)))
chdir('./esci-data/shopping_queries_dataset')

import pandas as pd

df_products = pd.read_parquet('shopping_queries_dataset_products.parquet')
df_products = df_products['us' == df_products.product_locale]
post_size = len(df_products) // 100

for i in ('5', '6'):
    print(i)
    buffer, start = [], time()
    for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
        buffer.append({'product_id': product_id, 'product_title': product_title})
        if post_size <= len(buffer):
            update(buffer, verbose=False)
            buffer = []
    if 0 < len(buffer):
        update(buffer, verbose=False)
    print('Elapsed Time: {0} (s)'.format(time() - start))
