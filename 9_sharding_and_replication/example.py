#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor
from requests import get
from requests import post

import json


def queries():
    response = get('http://127.0.0.1:8080/queries')
    return json.loads(response.text)['success']


def select(query):
    get('http://127.0.0.1:8080/select', params={'query': query})


def two_stage_select(query):
    get('http://127.0.0.1:8080/two_stage_select', params={'query': query})


def update(json_object):
    post('http://127.0.0.1:8080/update', data=json.dumps(json_object))


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


def do_8_5():
    for _ in range(2):
        buffer, start = [], time()

        for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
            buffer.append({'product_id': product_id, 'product_title': product_title})
            if post_size <= len(buffer):
                update(buffer)
                buffer = []

        if 0 < len(buffer):
            update(buffer)
        print('Elapsed Time: {0} (s)'.format(time() - start))


do_8_5()

print('4')


def do_8_6(max_workers=1):
    def fn(query):
        select(query)

    start = time()

    with ThreadPoolExecutor(max_workers=max_workers) as tpe:
        for query in queries():
            tpe.submit(fn, query)

    print('Elapsed Time: {0} (s)'.format(time() - start))


do_8_6()

print('5')

start = time()

for query in queries():
    two_stage_select(query)

print('Elapsed Time: {0} (s)'.format(time() - start))

print('6')

get('http://127.0.0.1:8080/truncate', params={'new_replicas': '2'})

do_8_5()

print('7')

do_8_6(8)
