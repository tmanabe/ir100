#!/usr/bin/env python3

from os import system

# 0

assert 0 == system('''curl -X POST http://127.0.0.1:8080/update --data-binary @- << EOS
[
    {"product_id": "x", "product_title": "a b c"},
    {"product_id": "y", "product_title": "b c d"},
    {"product_id": "z", "product_title": "c d e"}
]
EOS''')

assert 0 == system('curl http://127.0.0.1:8080/select?query=a')
assert 0 == system('curl http://127.0.0.1:8080/select?query=b')
assert 0 == system('curl http://127.0.0.1:8080/select?query=c')
