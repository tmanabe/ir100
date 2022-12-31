#!/usr/bin/env python3

from os import system

print('0')
assert 0 == system('''curl -X POST http://127.0.0.1:8080/update --data-binary @- << EOS
[
    {"product_id": "x", "product_title": "a b c"},
    {"product_id": "y", "product_title": "a b"},
    {"product_id": "z", "product_title": "a"}
]
EOS''')
assert 0 == system('curl http://127.0.0.1:8080/select?query=b')

print('1')
assert 0 == system('''curl -X POST http://127.0.0.1:8080/update --data-binary @- << EOS
[
    {"product_id": "v", "product_title": "c c"},
    {"product_id": "w", "product_title": "c c c"}
]
EOS''')
assert 0 == system('curl http://127.0.0.1:8080/select?query=c')

print('2')
assert 0 == system('curl http://127.0.0.1:8080/delete?product_id=v')
assert 0 == system('curl http://127.0.0.1:8080/select?query=c')

print('3')
assert 0 == system('''curl -X POST http://127.0.0.1:8080/update --data-binary @- << EOS
[
    {"product_id": "x", "product_title": "a b a b"},
    {"product_id": "y", "product_title": "c c c c"}
]
EOS''')
assert 0 == system('curl http://127.0.0.1:8080/select?query=c')
