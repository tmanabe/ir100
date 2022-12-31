#!/usr/bin/env python3

from requests import get
from requests import post

import json


def update(json_object):
    response = post('http://127.0.0.1:8080/update', data=json.dumps(json_object))
    print(response.text)


def select(word):
    response = get('http://127.0.0.1:8080/select', params={'query': word})
    print(response.text)


def delete(product_id):
    response = get('http://127.0.0.1:8080/delete', params={'product_id': product_id})
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
