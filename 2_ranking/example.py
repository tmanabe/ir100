#!/usr/bin/env python3

from os import chdir
from os.path import dirname

chdir(dirname(dirname(__file__)))
chdir('./esci-data/shopping_queries_dataset')

import pandas as pd

df_products = pd.read_parquet('shopping_queries_dataset_products.parquet')
df_products = df_products['us' == df_products.product_locale].set_index('product_id', drop=False)

# 0.

inverted_index_title = {}
for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
    counter = {}
    for word in product_title.split():
        if word in counter:
            counter[word] += 1
        else:
            counter[word] = 1
    for word, count in counter.items():
        if word in inverted_index_title:
            inverted_index_title[word].append((product_id, count))
        else:
            inverted_index_title[word] = [(product_id, count)]

# 1.

import heapq


class PriorityQueue:
    def __init__(self, size):
        assert 0 < size
        self.body = []
        self.size = size

    def peek(self):
        return self.body[0]

    def push(self, item):
        if len(self.body) < self.size:
            heapq.heappush(self.body, item)
        else:
            if self.peek() < item:
                heapq.heappushpop(self.body, item)

    def pop(self):
        return heapq.heappop(self.body)


priority_queue = PriorityQueue(10)

# 2.


def boolean_or_iterator(list_i, list_j):
    i, j = 0, 0
    try:
        while True:
            if list_i[i] < list_j[j]:
                yield(list_i[i][0], list_i[i][1], 0)
                i += 1
            elif list_j[j] < list_i[i]:
                yield(list_j[j][0], 0, list_j[j][1])
                j += 1
            else:
                yield(list_i[i][0], list_i[i][1], list_j[j][1])
                i += 1
                j += 1
    except IndexError:
        if i < len(list_i):
            for product_id, tf_i in list_i[i:]:
                yield (product_id, tf_i, 0)
        if j < len(list_j):
            for product_id, tf_j in list_j[j:]:
                yield (product_id, 0, tf_j)


print ('3.')

for product_id, tf_hdmi, tf_cable in boolean_or_iterator(inverted_index_title['HDMI'], inverted_index_title['Cable']):
    priority_queue.push((tf_hdmi + tf_cable, product_id))
while 0 < len(priority_queue.body):
    priority, product_id = priority_queue.pop()
    print(priority, product_id, df_products.at[product_id, 'product_title'])

print('4.')

from math import log

N = len(df_products)


def idf(n):
    return log((N - n + 0.5) / (n + 0.5))


df_hdmi = len(inverted_index_title['HDMI'])
df_cable = len(inverted_index_title['Cable'])

for product_id, tf_hdmi, tf_cable in boolean_or_iterator(inverted_index_title['HDMI'], inverted_index_title['Cable']):
    priority_queue.push((tf_hdmi * idf(df_hdmi) + tf_cable * idf(df_cable), product_id))
while 0 < len(priority_queue.body):
    priority, product_id = priority_queue.pop()
    print(priority, product_id, df_products.at[product_id, 'product_title'])

# 5.

info_title = {}
sum_length_title = 0

for product_id, product_title in zip(df_products['product_id'], df_products['product_title']):
    length_title = len(product_title.split())
    info_title[product_id] = length_title
    sum_length_title += length_title

avg_length_title = sum_length_title / N

print('6.')

K1, B = 1.2, 0.75


def bm25_weight(tf, length, avg_length):
    return (tf * (K1 + 1)) / (tf + K1 * (1 - B + B * length / avg_length))


for product_id, tf_hdmi, tf_cable in boolean_or_iterator(inverted_index_title['HDMI'], inverted_index_title['Cable']):
    length_title = info_title[product_id]
    bm25 = 0
    bm25 += bm25_weight(tf_hdmi, length_title, avg_length_title) * idf(df_hdmi)
    bm25 += bm25_weight(tf_cable, length_title, avg_length_title) * idf(df_cable)
    priority_queue.push((bm25, product_id, length_title))
while 0 < len(priority_queue.body):
    priority, product_id, length_title = priority_queue.pop()
    print(priority, product_id, length_title, df_products.at[product_id, 'product_title'])

print('7.')

df_basics = len(df_products['Amazon Basics' == df_products.product_brand])


def bm25f_weight(tf, boost, length, avg_length):
    return tf * boost / (1 - B + B * length / avg_length)

def answer7():
    for product_id, tf_hdmi, tf_cable in boolean_or_iterator(inverted_index_title['HDMI'], inverted_index_title['Cable']):
        boost_title, length_title = 2.0, info_title[product_id]
        weight_hdmi = bm25f_weight(tf_hdmi, boost_title, length_title, avg_length_title)
        weight_cable = bm25f_weight(tf_cable, boost_title, length_title, avg_length_title)
        tf_basics = 'Amazon Basics' == df_products.at[product_id, 'product_brand']
        boost_brand, length_brand, avg_length_brand = 1.0, 1, 1
        weight_basics = bm25f_weight(tf_basics, boost_brand, length_brand, avg_length_brand)
        bm25f = 0
        bm25f += weight_hdmi / (K1 + weight_hdmi) * idf(df_hdmi)
        bm25f += weight_cable / (K1 + weight_cable) * idf(df_cable)
        bm25f += weight_basics / (K1 + weight_basics) * idf(df_basics)
        priority_queue.push((bm25f, product_id, length_title))
    while 0 < len(priority_queue.body):
        priority, product_id, length_title = priority_queue.pop()
        print(priority, product_id, length_title, df_products.at[product_id, 'product_brand'], df_products.at[product_id, 'product_title'])


answer7()

print('8.')

K1 = 3.5
answer7()
