#!/usr/bin/env python3

from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from hashlib import md5
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer
from requests import get
from requests import post
from socketserver import ThreadingMixIn
from threading import current_thread
from urllib.parse import parse_qs
from urllib.parse import urlparse

import heapq
import json


# Cf. 2.2
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


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass


class Router(BaseHTTPRequestHandler):
    @classmethod
    def run(cls, arg_dict):
        cls.arg_dict = arg_dict
        cls.tpe = ThreadPoolExecutor(arg_dict.replicas)
        ThreadedHTTPServer((arg_dict.host, arg_dict.port), cls).serve_forever()

    def do_POST(self):
        response, result, contents = 200, {}, self.rfile.read(int(self.headers['content-length'])).decode('utf-8')

        if self.path.startswith('/update'):
            sharded_contents = []
            for _ in range(Router.arg_dict.shards):
                sharded_contents.append([])
            for product in json.loads(contents):
                product_hash = int(md5(product['product_id'].encode('utf-8')).hexdigest(),  16)
                sharded_contents[product_hash % Router.arg_dict.shards].append(product)

            port = Router.arg_dict.port
            with ThreadPoolExecutor(max_workers=Router.arg_dict.shards * Router.arg_dict.replicas) as tpe:
                def fn(url, contents):
                    return {url: json.loads(post(url, data=json.dumps(contents)).text)}

                fs = []
                for contents in sharded_contents:
                    for _ in range(Router.arg_dict.replicas):
                        port += 1
                        url = 'http://{0}:{1}/update'.format(Router.arg_dict.host, port)
                        fs.append(tpe.submit(fn, url=url, contents=contents))
                result['success'] = {}
                for f in as_completed(fs):
                    result['success'].update(f.result())

        else:
            response, result['error'] = 404, 'unknown POST endpoint'

        self.send(response, result)

    def do_GET(self):
        response, result, parameters = 200, {}, parse_qs(urlparse(self.path).query)

        if self.path.startswith('/queries'):
            queries, port = set(), Router.arg_dict.port + 1
            for _ in range(Router.arg_dict.shards):
                url = 'http://{0}:{1}/queries'.format(Router.arg_dict.host, port)
                queries |= set(json.loads(get(url).text)['success'])
                port += Router.arg_dict.replicas
            result['success'] = sorted(queries)

        elif self.path.startswith('/select'):
            def fn(query):
                result, replica_index = [], int(current_thread().name.rsplit('-', 1)[-1])
                with ThreadPoolExecutor(max_workers=Router.arg_dict.shards) as tpe:
                    def fn(query, shard_index, replica_index):
                        port = Router.arg_dict.port + 1 + shard_index * Router.arg_dict.replicas + replica_index
                        url = 'http://{0}:{1}/select'.format(Router.arg_dict.host, port)
                        return json.loads(get(url, params={'query': query}).text)['success']

                    fs = []
                    for shard_index in range(Router.arg_dict.shards):
                        fs.append(tpe.submit(fn, query, shard_index, replica_index))
                    for f in as_completed(fs):
                        result.append(f.result())
                return result

            assert 1 == len(parameters['query'])
            priority_queue, query = PriorityQueue(10), parameters['query'][0]
            for partial_result in Router.tpe.submit(fn, query).result():
                for product in partial_result:
                    priority_queue.push((product['_priority'], product['product_id'], product))
            ranking = []
            while 0 < len(priority_queue.body):
                _, _, product = priority_queue.pop()
                ranking.append(product)
            result['success'] = ranking

        elif self.path.startswith('/truncate'):
            port = Router.arg_dict.port
            result['success'] = {}
            for _ in range(Router.arg_dict.shards):
                for _ in range(Router.arg_dict.replicas):
                    port += 1
                    url = 'http://{0}:{1}/truncate'.format(Router.arg_dict.host, port)
                    result['success'][url] = json.loads(get(url).text)
            if 'new_shards' in parameters:
                assert 1 == len(parameters['new_shards'])
                Router.arg_dict.shards = int(parameters['new_shards'][0])
            if 'new_replicas' in parameters:
                assert 1 == len(parameters['new_replicas'])
                Router.arg_dict.replicas = int(parameters['new_replicas'][0])
                Router.tpe = ThreadPoolExecutor(Router.arg_dict.replicas)

        elif self.path.startswith('/two_stage_select'):
            def fn_select(query):
                result, replica_index = {}, int(current_thread().name.rsplit('-', 1)[-1])
                with ThreadPoolExecutor(max_workers=Router.arg_dict.shards) as tpe:
                    def fn(query, shard_index, replica_index):
                        port = Router.arg_dict.port + 1 + shard_index * Router.arg_dict.replicas + replica_index
                        url = 'http://{0}:{1}/select'.format(Router.arg_dict.host, port)
                        return {shard_index: json.loads(get(url, params={'omit_detail': 'y', 'query': query}).text)['success']}

                    fs = []
                    for shard_index in range(Router.arg_dict.shards):
                        fs.append(tpe.submit(fn, query, shard_index, replica_index))
                    for f in as_completed(fs):
                        result.update(f.result())
                return result

            def fn_fetch(shard_index_to_product_ids):
                result, replica_index = {}, int(current_thread().name.rsplit('-', 1)[-1])
                with ThreadPoolExecutor(max_workers=Router.arg_dict.shards) as tpe:
                    def fn(shard_index, product_ids):
                        port = Router.arg_dict.port + 1 + shard_index * Router.arg_dict.replicas + replica_index
                        url = 'http://{0}:{1}/fetch'.format(Router.arg_dict.host, port)
                        product_titles = json.loads(get(url, params={'product_id': product_ids}).text)['success']
                        return dict(zip(product_ids, product_titles))

                    fs = []
                    for shard_index, product_ids in shard_index_to_product_ids.items():
                        fs.append(tpe.submit(fn, shard_index, product_ids))
                    for f in as_completed(fs):
                        result.update(f.result())
                return result

            assert 1 == len(parameters['query'])
            priority_queue, query = PriorityQueue(10), parameters['query'][0]
            for shard_index, partial_result in Router.tpe.submit(fn_select, query).result().items():
                for product in partial_result:
                    assert 'product_title' not in product
                    priority_queue.push((product['_priority'], product['product_id'], shard_index, product))
            shard_index_to_product_ids, ranking = {}, []
            while 0 < len(priority_queue.body):
                _, product_id, shard_index, product = priority_queue.pop()
                if shard_index in shard_index_to_product_ids:
                    shard_index_to_product_ids[shard_index].append(product_id)
                else:
                    shard_index_to_product_ids[shard_index] = [product_id]
                ranking.append(product)
            product_id_to_title = Router.tpe.submit(fn_fetch, shard_index_to_product_ids).result()
            for product in ranking:
                product['product_title'] = product_id_to_title[product['product_id']]
            result['success'] = ranking

        else:
            response, result['error'] = 404, 'unknown GET endpoint'

        self.send(response, result)

    def send(self, response, result):
        self.send_response(response)
        self.send_header('Content-Type', 'text/json')
        self.end_headers()
        self.wfile.write(json.dumps(result, indent=4).encode('utf-8'))


if __name__ == '__main__':
    argument_parser = ArgumentParser(description='runs a router')
    argument_parser.add_argument('--host', default='127.0.0.1', help='host name', metavar='str', type=str)
    argument_parser.add_argument('--port', default=8080, help='port number', metavar='int', type=int)
    argument_parser.add_argument('--replicas', default=1, help='number of replicas', metavar='int', type=int)
    argument_parser.add_argument('--shards', default=2, help='number of shards', metavar='int', type=int)
    Router.run(argument_parser.parse_args())
